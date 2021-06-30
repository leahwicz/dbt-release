from dataclasses import replace
from pathlib import Path
from typing import List, Iterator, Optional
import shutil

from .cmd import stream_output, collect_output
from .common import EnvironmentInformation, ReleaseFile, PytestRunner
from .git import DbtRepository
from .virtualenvs import EnvBuilder, DevelopmentWheelEnv, DBTPackageEnv, PackagingEnv


class PypiBuilder:
    _SUBPACKAGES = (
        "core",
        "plugins/postgres",
        "plugins/redshift",
        "plugins/bigquery",
        "plugins/snowflake",
    )

    def __init__(self, dbt_path: Path, pkg_env_path: Path):
        self.dbt_path = dbt_path
        self.pkg_env_path = pkg_env_path.absolute()

    @staticmethod
    def _dist_for(path: Path, make=False) -> Path:
        dist_path = path / "dist"
        if dist_path.exists():
            shutil.rmtree(dist_path)
        if make:
            dist_path.mkdir(parents=True, exist_ok=True)
        build_path = path / "build"
        if build_path.exists():
            shutil.rmtree(build_path)
        return dist_path

    def _build_pypi_package(self, path: Path):
        print(f"building package in {path}")
        env_python = self.pkg_env_path / "bin" / "python"
        cmd = [env_python, "setup.py", "sdist", "bdist_wheel"]
        stream_output(cmd, cwd=path)
        print(f"finished building package in {path}")

    @staticmethod
    def _all_packages_in(path: Path) -> Iterator[Path]:
        path = path / "dist"
        for pattern in ("*.tar.gz", "*.whl"):
            yield from path.glob(pattern)

    def built_packages(self) -> Iterator[Path]:
        return self._all_packages_in(self.dbt_path)

    def _build_subpackage(self, name: str) -> Iterator[Path]:
        subpath = self.dbt_path / name
        self._dist_for(subpath)
        self._build_pypi_package(subpath)
        return self._all_packages_in(subpath)

    def build(self):
        print("building pypi packages")
        dist_path = self._dist_for(self.dbt_path)
        sub_pkgs: List[Path] = []
        for path in self._SUBPACKAGES:
            sub_pkgs.extend(self._build_subpackage(path))

        # now build the main package
        self._build_pypi_package(self.dbt_path)
        # now copy everything from the subpackages in
        for package in sub_pkgs:
            shutil.copy(str(package), dist_path)

        print("built pypi packages")

    def write_wheel_ordering(self, dest_path: Path):
        dbtenv = DBTPackageEnv(package_dir=self.dbt_path / "dist")
        pkgs = dbtenv.get_pkg_install_order()
        with dest_path.open("w") as fp:
            for path in pkgs:
                fp.write(f"./dist/{path.name}\n")

    def store_artifacts(self, env: EnvironmentInformation):
        artifact_dist_dir = env.dist_dir
        print(f"storing packaging artifacts in {artifact_dist_dir}")
        artifact_dist_dir.mkdir(parents=True, exist_ok=True)
        for path in self.built_packages():
            shutil.copyfile(path, artifact_dist_dir / path.name)
            print(f"Copied {path.name} to {artifact_dist_dir}")
        print("stored all packaging artifacts")
        self.write_wheel_ordering(env.wheel_file)


class WheelManager:
    """Manage the installing, testing and uploading of wheels."""

    def __init__(self, wheel_dir: Path, dbt_dir: Path):
        self.wheel_dir = wheel_dir
        self.dbt_dir = dbt_dir
        self.test_root = self.dbt_dir / "test/integration"

    def wheel_paths(self):
        return DBTPackageEnv(package_dir=self.wheel_dir).packages

    def install(
        self, env_path: Path, requirements: Path, dev_requirements: Optional[Path]
    ):
        virtualenv: EnvBuilder
        if dev_requirements is not None:
            virtualenv = DevelopmentWheelEnv(
                package_dir=self.wheel_dir,
                requirements=requirements,
                dev_requirements=dev_requirements,
            )
        else:
            virtualenv = DBTPackageEnv(
                package_dir=self.wheel_dir, requirements=requirements
            )
        virtualenv.create(env_path)

    def test(self, env_path: Path, name: str):
        runner = PytestRunner(env_path=env_path, dbt_path=self.dbt_dir)
        return runner.test(name)

    def upload(self):
        # to use this with the pypitest repository, either export the
        # environment variable TWINE_REPOSITORY=pypitest if you have a pypirc,
        # or  all of TWINE_REPOSITORY_URL, TWINE_USERNAME, and TWINE_PASSWORD
        # environment variables to your test information.
        cmd = ["twine", "check"]
        cmd.extend(str(p) for p in self.wheel_paths())
        stream_output(cmd)
        cmd = ["twine", "upload"]
        cmd.extend(str(p) for p in self.wheel_paths())
        print("uploading packages: {}".format(" ".join(cmd)))
        stream_output(cmd)
        print("uploaded packages")

    @classmethod
    def from_env_info(cls, env: EnvironmentInformation) -> "WheelManager":
        return cls(env.dist_dir, env.dbt_dir)


def make_requirements_txt(env_dir: Path, dbt_dir: Path, requirements_path: Path):
    """pip install the 'requirements.txt' file in the branch into a new
    virtualenv and collect all the non-dbt dependencies.
    """
    print("Generating requirements.txt file")
    reqenv = EnvBuilder(with_pip=True, upgrade_deps=True)
    reqenv.create(env_dir)
    pip = str(env_dir / "bin/pip")
    cmd = [pip, "install", "-r", "requirements.txt"]
    stream_output(cmd, cwd=dbt_dir)
    stdout = collect_output([pip, "freeze", "-l"], cwd=dbt_dir)
    with requirements_path.open("w") as fp:
        for line in stdout.split("\n"):
            if not line:
                continue
            parts = line.split("==")
            if len(parts) != 2:
                raise ValueError(f"Invalid requirements line: {line}")
            if parts[0] == "dbt" or parts[0].startswith("dbt-"):
                continue
            fp.write(line + "\n")
    print(f"Wrote requirements.txt file to {requirements_path}")


def set_output(name: str, value: str):
    print(f"setting {name}={value}")
    print(f"::set-output name={name}::{value}")


def create_build_commit(args=None):
    env = EnvironmentInformation()
    release = ReleaseFile.from_git()
    release.store_artifacts(env)
    pkgenv = PackagingEnv()
    pkgenv.create(env.packaging_venv)

    repository = DbtRepository(env.dbt_dir)
    repository.clone(branch=release.branch)

    # git checkout the release name
    repository.checkout_branch(release.release_branch_name, new=True)

    requirements_path = env.get_dbt_requirements_file(str(release.version))

    make_requirements_txt(env.linux_requirements_venv, env.dbt_dir, requirements_path)

    new_commit = repository.perform_version_update(
        release, requirements_path, env.packaging_venv
    )
    replace(release, commit=new_commit).store_artifacts(env)
    if args is None or args.push_updates:
        repository.push_updates(origin_name=release.release_branch_name)

    set_output("DBT_RELEASE_VERSION", str(release.version))
    set_output("DBT_RELEASE_COMMIT", release.commit)
    set_output("DBT_RELEASE_BRANCH", release.release_branch_name)


def set_env(name: str, value: str):
    print(f"::set-env name={name}::{value}")


def build_wheels(args=None):
    env = EnvironmentInformation()
    pkgenv = PackagingEnv()
    pkgenv.create(env.packaging_venv)
    pypi_builder = PypiBuilder(env.dbt_dir, env.packaging_venv)
    pypi_builder.build()
    pypi_builder.store_artifacts(env)


def merge_pr(args=None):
    print("Merging the temporary branch into the release branch")
    env = EnvironmentInformation()

    release = ReleaseFile.from_artifacts(env)
    repository = DbtRepository(env.dbt_dir)
    repository.clone(branch=release.branch)
    repository.merge(release.release_branch_name)
    repository.push_updates()

    # set the branch, and also set the others so other steps can just rely on
    # this
    set_output("DBT_RELEASE_VERSION", str(release.version))
    set_output("DBT_RELEASE_COMMIT", release.commit)
    set_output("DBT_RELEASE_BRANCH", release.branch)


def test_wheels(args=None):
    if args is None:
        target = "postgres"
    else:
        target = args.test_name

    env = EnvironmentInformation()

    release = ReleaseFile.from_artifacts(env)

    tester = WheelManager.from_env_info(env)
    requirements = env.get_dbt_requirements_file(str(release.version))
    dev_requirements = env.dbt_dir / "dev-requirements.txt"
    tester.install(
        env.test_venv, requirements=requirements, dev_requirements=dev_requirements
    )
    tester.test(env.test_venv, target)


def upload_artifacts(args=None):
    env = EnvironmentInformation()

    tester = WheelManager.from_env_info(env)
    tester.upload()


def add_native_parsers(subparsers):
    native_sub = subparsers.add_parser("native", help="build the wheels/tarfiles")
    native_subs = native_sub.add_subparsers(title="Available sub-commands")

    create_sub = native_subs.add_parser(
        "create",
        help=(
            "Create the release commit and store an updated release file for "
            "that commit in artifacts"
        ),
    )
    create_sub.add_argument("--no-push", dest="push_updates", action="store_false")
    create_sub.set_defaults(func=create_build_commit)

    pkg_sub = native_subs.add_parser("package", help="build the wheels/tarfiles")
    pkg_sub.set_defaults(func=build_wheels)

    test_sub = native_subs.add_parser("test", help="Run the given tests")
    test_sub.add_argument(
        "test_name", choices=["rpc", "postgres", "redshift", "bigquery", "snowflake"]
    )
    test_sub.set_defaults(func=test_wheels)

    merge_sub = native_subs.add_parser(
        "merge", help="Merge the temporary release branch"
    )
    merge_sub.set_defaults(func=merge_pr)

    upload_sub = native_subs.add_parser("upload", help="Upload the package to pypi")
    upload_sub.set_defaults(func=upload_artifacts)
