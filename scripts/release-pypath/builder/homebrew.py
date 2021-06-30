from dataclasses import dataclass, replace
from pathlib import Path
from typing import List, Iterator, Tuple
from urllib.error import HTTPError
from urllib.request import urlopen
import abc
import hashlib
import json
import os
import pickle
import tarfile
import textwrap
import time


from .cmd import collect_output, stream_output
from .common import (
    PackageType,
    Version,
    EnvironmentInformation,
    ReleaseFile,
    PytestRunner,
)
from .git import HomebrewRepository
from .virtualenvs import DBTPackageEnv


@dataclass
class HomebrewDependency:
    name: str
    url: str
    sha256: str
    version: str

    def render(self, indent: int = 2) -> str:
        result = textwrap.dedent(
            f"""\
            resource "{self.name}" do # {self.name}=={self.version}
              url "{self.url}"
              sha256 "{self.sha256}"
            end
        """
        )
        return textwrap.indent(result, " " * indent)

    def __str__(self) -> str:
        return self.render(indent=0)


@dataclass
class HomebrewTemplate:
    dbt_package: HomebrewDependency
    dbt_dependencies: List[HomebrewDependency]
    ext_dependencies: List[HomebrewDependency]

    def verify_versions(self, version: str):
        for pkg in [self.dbt_package] + self.dbt_dependencies:
            if pkg.version != str(version):
                raise ValueError(
                    f"Found an invalid {pkg.name}=={pkg.version}, "
                    f"expected {pkg.name}=={version}"
                )

    @classmethod
    def from_dependencies(
        cls, packages: Iterator[HomebrewDependency], version: str
    ) -> "HomebrewTemplate":
        dbt_dependencies = []
        ext_dependencies = []
        dbt_package = None
        for pkg in packages:
            if pkg.name == "dbt":
                dbt_package = pkg
            # we can assume that anything starting with dbt- in a fresh
            # venv is a dbt package, except dbt-extractor.
            # when adapter plugins get split up, this janky check will go away.
            elif pkg.name.startswith("dbt-") and pkg.name != "dbt-extractor":
                dbt_dependencies.append(pkg)
            else:
                ext_dependencies.append(pkg)
        if dbt_package is None:
            raise RuntimeError('never found dbt in "pip freeze -l" output')
        template = cls(
            dbt_package=dbt_package,
            ext_dependencies=ext_dependencies,
            dbt_dependencies=dbt_dependencies,
        )
        template.verify_versions(version)
        return template

    def to_formula(self, version: Version, versioned: bool = True):
        fmt = self._dbt_homebrew_formula_fmt()

        if versioned:
            formula_name = version.homebrew_class_name()
        else:
            formula_name = "Dbt"

        dependencies = self.dbt_dependencies + self.ext_dependencies
        dependencies_str = "\n".join(d.render() for d in dependencies)

        return fmt.format(
            formula_name=formula_name,
            version=version,
            url_data=self.dbt_package.url,
            hash_data=self.dbt_package.sha256,
            dependencies=dependencies_str,
        )

    @staticmethod
    def _dbt_homebrew_formula_fmt() -> str:
        return textwrap.dedent(
            """\
            class {formula_name} < Formula
              include Language::Python::Virtualenv

              desc "Data build tool"
              homepage "https://github.com/fishtown-analytics/dbt"
              url "{url_data}"
              sha256 "{hash_data}"
              revision 1

              bottle do
                root_url "http://bottles.getdbt.com"
                # bottle hashes + versions go here
              end

              depends_on "rust" => :build
              depends_on "libffi"
              depends_on "openssl@1.1"
              depends_on "postgresql"
              depends_on "python@3.8"

            {dependencies}
              def install
                venv = virtualenv_create(libexec, "python3")
                venv.instance_variable_get(:@formula).system venv.instance_variable_get(:@venv_root)/"bin/pip", "install",
                  "--upgrade", "pip"

                resources.each do |r|
                  if r.name == "snowflake-connector-python"
                    # workaround for installing `snowflake-connector-python`
                    # package w/o build-system deps (e.g. pyarrow)
                    # adds the `--no-use-pep517` parameter
                    r.stage do
                      venv.instance_variable_get(:@formula).system venv.instance_variable_get(:@venv_root)/"bin/pip", "install",
                        "-v", "--no-deps", "--no-binary", ":all:", "--ignore-installed", "--no-use-pep517", Pathname.pwd
                    end
                  else
                    venv.pip_install r
                  end
                end

                venv.pip_install_and_link buildpath

                bin.install_symlink "#{{libexec}}/bin/dbt" => "dbt"
              end

              test do
                (testpath/"dbt_project.yml").write(
                  "{{name: 'test', version: '0.0.1', config-version: 2, profile: 'default'}}",
                )
                (testpath/".dbt/profiles.yml").write(
                  "{{default: {{outputs: {{default: {{type: 'postgres',
                  threads: 1, host: 'localhost', port: 5432, user: 'root',
                  pass: 'password', dbname: 'test', schema: 'test'}}}},
                  target: 'default'}}}}",
                )
                (testpath/"models/test.sql").write("select * from test")
                system "#{{bin}}/dbt", "test"
              end
            end
            """
        )

    def store_artifacts(self, env: EnvironmentInformation) -> None:
        with open(env.homebrew_template_pickle, "wb") as fp:
            pickle.dump(self, fp)

    @classmethod
    def from_artifacts(cls, env: EnvironmentInformation) -> "HomebrewTemplate":
        with open(env.homebrew_template_pickle, "rb") as fp:
            template = pickle.load(fp)
        return template


# we have two builders: The local builder files with `file://` URLs pointing to
# the dists, and updates the hash. The Pypi builder updates that


class BaseHomebrewBuilder(metaclass=abc.ABCMeta):
    def __init__(self, version: Version, env_path: Path, homebrew_path: Path):
        self.version = version
        self.env_path = env_path
        self.homebrew_path = homebrew_path

    @property
    def versioned_formula_path(self) -> Path:
        return self.homebrew_path / "Formula" / self.version.homebrew_filename()

    @property
    def default_formula_path(self) -> Path:
        return self.homebrew_path / "Formula/dbt.rb"

    def get_pypi_info(self, pkg: str, version: str) -> Tuple[str, str]:
        url = f"https://pypi.org/pypi/{pkg}/{version}/json"
        try:
            fp = urlopen(url)
        except Exception as exc:
            print(f"Could not get pypi info for url {url}: {exc}")
            raise
        try:
            data = json.load(fp)
        finally:
            fp.close()
        assert "urls" in data
        for pkginfo in data["urls"]:
            assert "packagetype" in pkginfo
            if pkginfo["packagetype"] == "sdist":
                assert "url" in pkginfo
                assert "digests" in pkginfo
                assert "sha256" in pkginfo["digests"]
                url = pkginfo["url"]
                sha256 = pkginfo["digests"]["sha256"]
                return url, sha256
        raise ValueError(f"Never got a valid sdist for {pkg}=={version}")

    def wait_for_pypi_info(self, pkg: str, version: str) -> Tuple[str, str]:
        # get info from pypi, retrying a few times on 404s and sleeping.
        # Once you upload a package to pypi, it will likely 404 for a
        # non-deterministic amount of time
        attempts = 5
        sleeptime = 30
        while attempts:
            attempts -= 1
            try:
                return self.get_pypi_info(pkg=pkg, version=version)
            except HTTPError as exc:
                if exc.code == 404 and attempts:
                    print(f"retrying failed query, {attempts} attempts remaining")
                    time.sleep(sleeptime)
                    continue
                else:
                    raise

    def get_pip_versions(self, env_path: Path) -> Iterator[Tuple[str, str]]:
        pip = env_path / "bin/pip"
        cmd = [pip, "freeze", "-l"]
        raw = collect_output(cmd).split("\n")
        for line in raw:
            if not line:
                continue
            parts = line.split("==")
            if len(parts) != 2:
                raise ValueError(f"Could not parse pip freeze output line: {line}")
            name, version = parts
            yield name, version

    def create_versioned_formula_file(self, template: HomebrewTemplate):
        formula_contents = template.to_formula(self.version, versioned=True)
        if self.versioned_formula_path.exists():
            print("Homebrew formula path already exists, overwriting")
        self.versioned_formula_path.write_text(formula_contents)

    def create_default_formula_file(self, template: HomebrewTemplate):
        formula_contents = template.to_formula(self.version, versioned=False)
        self.default_formula_path.write_text(formula_contents)

    @staticmethod
    def uninstall_reinstall_basics(formula_path: Path, audit: bool = True):
        path = os.path.normpath(formula_path)
        stream_output(["brew", "uninstall", "--force", "--formula", path])
        versions = []
        for line in collect_output(["brew", "list", "--formula"]).split("\n"):
            line = line.strip()
            if line.startswith("dbt@") or line == "dbt":
                versions.append(line)
        if versions:
            stream_output(["brew", "unlink"] + versions)
        stream_output(["brew", "install", path])
        stream_output(["brew", "test", path])
        if audit:
            stream_output(["brew", "audit", "--strict", "--formula", path])

    def _get_env_python_path(self) -> Path:
        magic = "python path: "
        # this is expected to return non-zero (no profile or project yml)
        output = collect_output(["dbt", "debug"], check=False)
        for line in output.split("\n"):
            if line.startswith(magic):
                return Path(line[len(magic) :])
        raise ValueError(f'Never found "{magic}" in output:\n{output}')

    @abc.abstractmethod
    def get_packages(self) -> Iterator[HomebrewDependency]:
        pass


def _tgz_to_name(path: Path) -> str:
    with tarfile.open(path) as archive:
        names = [n for n in archive.getnames() if n.split("/")[-1] == "PKG-INFO"]
        names.sort(key=lambda n: n.count("/"))
        for name in names:
            lines = archive.extractfile(name).read().split(b"\n")
            for line in lines:
                try:
                    linestr = line.decode("utf-8")
                except:  # noqa
                    continue  # probably invalid utf-8, whatever
                parts = linestr.split(": ", 1)
                if len(parts) == 2:
                    if parts[0] == "Name":
                        return parts[1]
    raise ValueError(f"Never found a package name for {path}")


class HomebrewLocalBuilder(BaseHomebrewBuilder):
    def __init__(
        self,
        version: Version,
        env_path: Path,
        homebrew_path: Path,
        dbt_path: Path,
        package_dir: Path,
    ) -> None:
        super().__init__(
            version=version, env_path=env_path, homebrew_path=homebrew_path
        )
        self.dbt_path = dbt_path
        self.package_dir = package_dir

    def make_venv(self, path: Path):
        # homebrew can't gracefully handle wheels
        env = DBTPackageEnv(package_dir=self.package_dir, ext=PackageType.Sdist)
        env.create(path)

    def get_template(self) -> HomebrewTemplate:
        self.make_venv(self.env_path)
        print("done setting up virtualenv")
        packages = self.get_packages()
        return HomebrewTemplate.from_dependencies(packages, str(self.version))

    @staticmethod
    def _sha256_at_path(path: Path) -> str:
        value = hashlib.sha256()
        value.update(path.read_bytes())
        return value.hexdigest()

    def get_packages(self) -> Iterator[HomebrewDependency]:
        dbt_tgzs = {_tgz_to_name(w): w for w in self.package_dir.glob("*.tar.gz")}

        for name, version in self.get_pip_versions(self.env_path):
            if name in dbt_tgzs:
                path = dbt_tgzs[name].absolute()
                url = f"file://{path}"
                sha256 = self._sha256_at_path(path)
            else:
                url, sha256 = self.get_pypi_info(name, version)
            dep = HomebrewDependency(name=name, url=url, sha256=sha256, version=version)
            yield dep

    def run_tests(self, formula_path: Path, audit: bool = True):
        self.uninstall_reinstall_basics(formula_path=formula_path, audit=audit)
        python_bin = self._get_env_python_path()
        dev_requirements = self.dbt_path / "dev-requirements.txt"
        stream_output([python_bin, "-m", "pip", "install", "-r", dev_requirements])
        env_path = python_bin.parent.parent
        runner = PytestRunner(env_path, self.dbt_path)
        # no postgres - it's too much work to set it up locally on macs and
        # this is just a very basic smoke test
        for name in ["bigquery", "redshift", "snowflake"]:
            runner.test(name)

    def test(self, template):
        self.create_versioned_formula_file(template)
        self.run_tests(self.versioned_formula_path)

    @classmethod
    def from_env_info(cls, env: EnvironmentInformation) -> "HomebrewLocalBuilder":
        release = ReleaseFile.from_artifacts(env)
        return cls(
            version=release.version,
            env_path=env.homebrew_test_venv,
            homebrew_path=env.homebrew_checkout_path,
            package_dir=env.dist_dir,
            dbt_path=env.dbt_dir,
        )


class HomebrewPypiBuilder(BaseHomebrewBuilder):
    def __init__(
        self,
        version: Version,
        env_path: Path,
        homebrew_path: Path,
        dbt_path: Path,
        set_default: bool,
    ) -> None:
        super().__init__(
            version=version, env_path=env_path, homebrew_path=homebrew_path
        )
        self.dbt_path = dbt_path
        self.set_default = set_default

    def get_packages(self) -> Iterator[HomebrewDependency]:
        for name, version in self.get_pip_versions(self.env_path):
            url, sha256 = self.get_pypi_info(name, version)
            dep = HomebrewDependency(name=name, url=url, sha256=sha256, version=version)
            yield dep

    @classmethod
    def from_env_info(cls, env: EnvironmentInformation) -> "HomebrewPypiBuilder":
        release = ReleaseFile.from_artifacts(env)
        return cls(
            version=release.version,
            env_path=env.homebrew_release_venv,
            homebrew_path=env.homebrew_checkout_path,
            dbt_path=env.dbt_dir,
            set_default=release.is_default_version,
        )

    def commit_versioned_formula(self):
        # add a commit for the new formula
        stream_output(
            ["git", "add", self.versioned_formula_path], cwd=self.homebrew_path
        )
        stream_output(
            ["git", "commit", "-m", f"add dbt@{self.version}"], cwd=self.homebrew_path
        )

    def commit_default_formula(self):
        stream_output(["git", "add", self.default_formula_path], cwd=self.homebrew_path)
        stream_output(
            ["git", "commit", "-m", f"upgrade dbt to {self.version}"],
            cwd=self.homebrew_path,
        )

    def _replaced_dep(self, dep: HomebrewDependency) -> HomebrewDependency:
        url, sha256 = self.wait_for_pypi_info(dep.name, dep.version)
        if sha256 != dep.sha256:
            # this should maybe be an error!
            print(
                "WARNING: Unexpected sha256.\n"
                "{dep.url} had sha256sum of {dep.sha256}, but\n"
                "{url} has sha256sum of {sha256}"
            )
        return replace(dep, url=url, sha256=sha256)

    def add_dbt_template(self, template: HomebrewTemplate) -> HomebrewTemplate:
        dbt_package = self._replaced_dep(template.dbt_package)
        dbt_dependencies = [
            self._replaced_dep(dep) for dep in template.dbt_dependencies
        ]
        return replace(
            template, dbt_package=dbt_package, dbt_dependencies=dbt_dependencies
        )

    def build_and_test(self, template: HomebrewTemplate):
        template = self.add_dbt_template(template)
        self.create_versioned_formula_file(template)
        self.uninstall_reinstall_basics(
            formula_path=self.default_formula_path, audit=False
        )
        self.commit_versioned_formula()

        if self.set_default:
            self.create_default_formula_file(template)
            self.uninstall_reinstall_basics(
                formula_path=self.default_formula_path, audit=False
            )
            self.commit_default_formula()


def homebrew_test(args=None):
    """Given the produced wheels, build a test homebrew formula and install it
    locally, running some tests.
    """
    env = EnvironmentInformation()
    repository = HomebrewRepository(env.homebrew_checkout_path)
    repository.clone()

    builder = HomebrewLocalBuilder.from_env_info(env=env)
    template = builder.get_template()
    template.store_artifacts(env=env)
    builder.test(template)


def homebrew_upload(args=None):
    env = EnvironmentInformation()
    repository = HomebrewRepository(env.homebrew_checkout_path)
    repository.clone()

    builder = HomebrewPypiBuilder.from_env_info(env=env)
    template = HomebrewTemplate.from_artifacts(env=env)
    builder.build_and_test(template=template)

    # push!
    repository.push_updates()


def add_homebrew_parsers(subparsers):
    homebrew_sub = subparsers.add_parser("homebrew", help="Homebrew operations")
    homebrew_subs = homebrew_sub.add_subparsers(title="Available sub-commands")

    homebrew_test_sub = homebrew_subs.add_parser(
        "test", help="Test the homebrew package"
    )
    homebrew_test_sub.set_defaults(func=homebrew_test)

    homebrew_upload_sub = homebrew_subs.add_parser(
        "upload", help="Upload the homebrew package"
    )
    homebrew_upload_sub.set_defaults(func=homebrew_upload)
