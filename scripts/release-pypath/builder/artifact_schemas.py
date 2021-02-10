import re
from dataclasses import dataclass
import textwrap
import json
import tempfile
from pathlib import Path
from .common import EnvironmentInformation, ReleaseFile
from .git import ArtifactSchemaRepository
from .cmd import stream_output, collect_output
from .virtualenvs import SchemaArtifactEnv
from urllib.request import urlopen
from typing import List


@dataclass
class SchemaInfo:
    name: str
    json_path: Path
    docs_path: Path

    def render(self) -> str:
        result = textwrap.dedent(
            f"""\
            <li>
              <a href="{self.json_path}">{self.name}</a> (<a href="{self.docs_path}">documentation</a>)
            </li>
        """
        )
        return result

    def __str__(self) -> str:
        return self.render()


def schema_artifacts_to_html(schema_data: List[SchemaInfo]) -> str:
    fmt = _get_index_html_template()

    artifact_schema_list = "\n".join([str(artifact) for artifact in schema_data])

    artifact_schema_html = textwrap.dedent(
        f"""\
        <ul>
          {artifact_schema_list}
        </ul>
    """
    )

    return fmt.format(artifact_schema_html=artifact_schema_html)


def _get_index_html_template() -> str:
    return textwrap.dedent(
        """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta http-equiv="content-type" content="text/html; charset=utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>dbt JSON Schema</title>
  <style>
body {{
  background: #003645!important;
  color: #93a1a1;
  font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Oxygen,Ubuntu,Cantarell,"Fira Sans","Droid Sans","Helvetica Neue",Helvetica,Arial,sans-serif;
  padding: 40px 20px;
  text-align: center;
}}
main {{
  font-size: 180%;
}}
h1 {{
  color: #fff;
}}
i {{
  color: #719e07;
}}
p {{
  margin-bottom: 2em;
}}
a {{
  color: #93a1a1;
  text-decoration: none;
}}
a:hover {{
  color: #fff
}}
ul {{
  list-style: none;
  padding: 0;

}}
ul li {{
  margin-bottom: 1em;
}}
footer {{
  margin-top: 10em;
  font-size: 140%;
}}
  </style>
</head>
<body>
  <main>

    <img src="static/dbt-logo-full-white.svg" width="150">
    <h1>dbt JSON Schema</h1>

    <p>Individual JSON Schemas for dbt artifact objects</p>

    {artifact_schema_html}
  </main>

  <footer>
    <p><a href="https://github.com/fishtown-analytics/schemas.getdbt.com">github.com/fishtown-analytics/schemas.getdbt.com</a></p>
  </footer>
</body>
</html>
"""
    )


def check_artifact_schema(args=None):
    env = EnvironmentInformation()
    artifact_env = SchemaArtifactEnv(env.dbt_dir / "requirements.txt")
    artifact_env.create(env.schemas_venv)
    pip = str(env.schemas_venv / "bin/pip")
    cmd = [pip, "install", "-r", "requirements.txt"]
    stream_output(cmd, cwd=env.dbt_dir)

    python_path = env.schemas_venv / "bin/python"
    schemas_dest_dir = env.build_dir / "schemas"
    stream_output(
        cmd=[
            str(python_path),
            "scripts/collect-artifact-schema.py",
            "--path",
            str(schemas_dest_dir),
        ],
        cwd=env.dbt_dir,
    )

    artifact_schema_repo = ArtifactSchemaRepository(env.schemas_checkout_path)
    artifact_schema_repo.clone()

    deep_diff_path = str(env.schemas_venv / "bin/deep")
    schema_files = schemas_dest_dir.glob("**/*.json")
    for schema_file_path in schema_files:
        with open(schema_file_path) as f:
            relative_path = re.sub(
                str(schemas_dest_dir) + "/", "", str(schema_file_path)
            )
            exists = (env.schemas_checkout_path / relative_path).exists()
            if not exists:
                print(f"Found new schema {relative_path}, skipping")
                continue
            schema_data = json.load(f)
            url = schema_data["$id"]
            with urlopen(url) as fp, tempfile.NamedTemporaryFile(
                suffix=".json"
            ) as temp_fp:
                temp_fp.write(fp.read())
                cmd = [
                    deep_diff_path,
                    "diff",
                    temp_fp.name,
                    schema_file_path,
                    "--ignore-order",
                    "--exclude-regex-paths",
                    "\\['dbt_version'\\]\\['default'\\]|"
                    "\\['generated_at'\\]\\['default'\\]|\\['description'\\]|"
                    "\\['dbt_schema_version'\\]\\['default'\\]",
                ]
                results = collect_output(cmd).strip()
                if results != "{}":
                    raise ValueError(
                        f"There are breaking changes to artifact schema {relative_path}:"
                        f"\n{results}"
                    )

    print("No breaking changes!")


def publish_artifact_schema(args=None):
    env = EnvironmentInformation()
    release = ReleaseFile.from_artifacts(env)
    artifact_env = SchemaArtifactEnv(env.dbt_dir / "requirements.txt")
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
            env.schemas_checkout_path,
        ],
        cwd=env.dbt_dir,
    )

    # generate docs
    generate_schema_doc_path = str(env.schemas_venv / "bin/generate-schema-doc")
    schema_files = (env.schemas_checkout_path / "dbt").glob("**/*.json")
    schema_data = []
    for schema_file_path in schema_files:
        schema_dir_path = schema_file_path.with_suffix("")
        schema_dir_path.mkdir(exist_ok=True)
        schema_docs_path = schema_dir_path / "index.html"
        cmd = [generate_schema_doc_path, schema_file_path, schema_docs_path]
        stream_output(cmd, cwd=env.schemas_checkout_path)
        schema_data.append(
            SchemaInfo(
                name=str(schema_file_path.relative_to(env.schemas_checkout_path)),
                docs_path=schema_docs_path.relative_to(env.schemas_checkout_path),
                json_path=schema_file_path.relative_to(env.schemas_checkout_path),
            )
        )

    index_file = env.schemas_checkout_path / "index.html"
    index_file.write_text(schema_artifacts_to_html(schema_data))

    stream_output(
        ["git", "add", env.schemas_checkout_path / "dbt", index_file],
        cwd=env.schemas_checkout_path,
    )
    stream_output(
        ["git", "commit", "-m", f"artifact schema updates for dbt@{release.version}"],
        cwd=env.schemas_checkout_path,
    )

    if args is None or args.push_updates:
        artifact_schema_repo.push_updates()


def add_artifact_schema_parsers(subparsers):
    artifact_schema_sub = subparsers.add_parser(
        "schemas", help="generate/check artifact schema"
    )
    artifact_schema_subs = artifact_schema_sub.add_subparsers(
        title="Available sub-commands"
    )

    check_sub = artifact_schema_subs.add_parser(
        "check",
        help=("Generates artifact schema and diff against published artifact schemas"),
    )
    check_sub.set_defaults(func=check_artifact_schema)

    publish_sub = artifact_schema_subs.add_parser(
        "publish", help="Publish artifact schema to schemas.getdbt.com"
    )
    publish_sub.add_argument("--no-push", dest="push_updates", action="store_false")
    publish_sub.set_defaults(func=publish_artifact_schema)
