import json
import tempfile
from pathlib import Path
from .common import EnvironmentInformation, ReleaseFile
from .git import ArtifactSchemaRepository
from .cmd import stream_output, collect_output
from .virtualenvs import ArtifactDiffEnv
from urllib.request import urlopen


def check_artifact_schema(args=None):
    env = EnvironmentInformation()
    artifact_env = ArtifactDiffEnv(env.dbt_dir / "requirements.txt")
    artifact_env.create(env.schemas_venv)
    pip = str(env.schemas_venv / "bin/pip")
    cmd = [pip, "install", "-r", "requirements.txt"]
    stream_output(cmd, cwd=env.dbt_dir)

    python_path = str(env.schemas_venv / "bin/python")
    stream_output(
        cmd=[
            python_path,
            "scripts/collect-artifact-schema.py",
            "--path",
            env.build_dir / "schemas"
        ],
        cwd=env.dbt_dir
    )

    deep_diff_path = str(env.schemas_venv / "bin/deep")
    artifact_files = Path(env.build_dir / "schemas").glob('**/*.json')
    for schema_file_path in artifact_files:
        with open(schema_file_path) as f:
            schema_data = json.load(f)
            url = schema_data["$id"]
            with urlopen(url) as fp, tempfile.NamedTemporaryFile(suffix='.json') as temp_fp:
                temp_fp.write(fp.read())

                cmd = [
                    deep_diff_path,
                    "diff",
                    schema_file_path,
                    temp_fp.name,
                    "--ignore-order",
                    "--exclude-regex-paths",
                    "\\['dbt_version'\\]\\['default'\\]|\\['generated_at'\\]\\['default'\\]|\\['description'\\]"
                ]
                results = collect_output(cmd).strip()
                if results != '{}':
                    raise ValueError(
                        f"There are breaking changes to artifact schema {url}:\n{results}")


def publish_artifact_schema(args=None):
    env = EnvironmentInformation()
    release = ReleaseFile.from_artifacts(env)
    artifact_env = ArtifactDiffEnv(env.dbt_dir / "requirements.txt")
    artifact_env.create(env.schemas_venv)
    pip = str(env.schemas_venv / "bin/pip")
    cmd = [pip, "install", "-r", "requirements.txt"]
    stream_output(cmd, cwd=env.dbt_dir)

    artifact_schema_repo = ArtifactSchemaRepository(env.schemas_checkout_path)
    artifact_schema_repo.clone()

    python_path = str(env.schemas_venv / "bin/python")
    stream_output(
        cmd=[
            python_path,
            "scripts/collect-artifact-schema.py",
            "--path",
            env.schemas_checkout_path
        ],
        cwd=env.dbt_dir
    )

    stream_output(
        ["git", "add", env.schemas_checkout_path / "dbt"], cwd=env.schemas_checkout_path
    )
    stream_output(
        ["git", "commit", "-m", f"artifact schema updates for dbt@{release.version}"], cwd=env.schemas_checkout_path
    )

    if args is None or args.push_updates:
        artifact_schema_repo.push_updates()


def add_artifact_schema_parsers(subparsers):
    artifact_schema_sub = subparsers.add_parser(
        "schemas", help="generate/check artifact schema")
    artifact_schema_subs = artifact_schema_sub.add_subparsers(
        title="Available sub-commands")

    check_sub = artifact_schema_subs.add_parser(
        "check",
        help=(
            "Generates artifact schema and diff against published artifact schemas"
        ),
    )
    check_sub.set_defaults(func=check_artifact_schema)

    publish_sub = artifact_schema_subs.add_parser(
        "publish", help="Publish artifact schema to schemas.getdbt.com")
    publish_sub.add_argument("--no-push", dest="push_updates", action="store_false")
    publish_sub.set_defaults(func=publish_artifact_schema)
