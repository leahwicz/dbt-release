#!/usr/bin/env python
from contextlib import contextmanager
from pathlib import Path
from typing import List, Iterator
import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import venv  # type: ignore

if sys.version_info < (3, 6):
    raise ValueError('Python 3.6 or greater required!')


DBT_REPO = 'git@github.com:fishtown-analytics/dbt.git'

# This should match the pattern in .bumpversion.cfg
VERSION_PATTERN = re.compile(
    r'(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)'
    r'((?P<prerelease>[a-z]+)(?P<num>\d+))?'
)


def stream_output(cmd, cwd=None) -> None:
    try:
        subprocess.run(
            cmd, cwd=cwd, check=True, stdout=None, stderr=None
        )
    except subprocess.CalledProcessError as exc:
        print(f'Command {exc.cmd} failed')
        if exc.output:
            print(exc.output.decode('utf-8'))
        if exc.stderr:
            print(exc.stderr.decode('utf-8'), file=sys.stderr)
        raise


def collect_output(cmd, cwd=None, stderr=subprocess.PIPE, check=True) -> str:
    try:
        result = subprocess.run(
            cmd, cwd=cwd, check=check, stdout=subprocess.PIPE, stderr=stderr
        )
    except subprocess.CalledProcessError as exc:
        print(f'Command {exc.cmd} failed')
        if exc.output:
            print(exc.output.decode('utf-8'))
        if exc.stderr:
            print(exc.stderr.decode('utf-8'), file=sys.stderr)
        raise
    return result.stdout.decode('utf-8')


def run_command(cmd, cwd=None) -> None:
    result = collect_output(cmd, stderr=subprocess.STDOUT, cwd=cwd)
    print(result)


# TODO: update the base image so this works on python 3.8, use dataclasses
class ReleaseFile:
    def __init__(
        self,
        path: Path,
        version: str,
        commit: str,
        branch: str,
        notes: str,
    ):
        self.path = path
        self.version = version
        self.commit = commit
        self.branch = branch
        self.notes = notes

    @classmethod
    def from_path(cls, path: Path) -> 'ReleaseFile':
        with path.open() as fp:
            first_lines = ''.join(fp.readline() for _ in range(3))
            notes = fp.read().strip()
        version = re.search(r'version: (.*)\n', first_lines)
        if not version:
            raise ValueError(
                f'No verison found in first 3 lines: {first_lines}'
            )
        version_str = version.group(1)
        if not VERSION_PATTERN.match(version_str):
            raise ValueError(
                f'Version {version_str} did not match required pattern'
            )
        commit = re.search(r'commit: (.*)\n', first_lines)
        if not commit:
            raise ValueError(
                f'No commit found in first 3 lines: {first_lines}'
            )

        branch = re.search(r'branch: (.*)\n', first_lines)
        if not branch:
            raise ValueError(
                f'No branch found in first 3 lines: {first_lines}'
            )
        return cls(
            path=path,
            version=version_str,
            commit=commit.group(1),
            branch=branch.group(1),
            notes=notes,
        )

    @staticmethod
    def _git_modified_files_last_commit() -> List[Path]:
        cmd = ['git', 'diff', '--name-status', 'HEAD~1', 'HEAD']
        result = collect_output(cmd)
        paths = []
        for line in result.strip().split('\n'):
            if line[0] not in 'AM':
                continue
            match = re.match(r'^[AM]\s*(releases/.*)', line)
            if match is None:
                continue
            path = Path(match.group(1))
            paths.append(path)
        return paths

    @classmethod
    def from_git(cls) -> 'ReleaseFile':
        found = []
        for path in cls._git_modified_files_last_commit():
            try:
                release = cls.from_path(path)
            except ValueError as exc:
                print(f'Found an invalid release file, skipping it: {exc}',
                      file=sys.stderr)
            else:
                found.append(release)
        if len(found) > 1:
            paths = ', '.join(str(r.path) for r in found)
            raise ValueError(
                f'Found {len(found)} releases (paths: [{paths}]), expected 1'
            )
        elif len(found) < 1:
            raise ValueError('Found 0 releases, expected 1')
        return found[0]


class DbtRepository:
    def __init__(self, path: Path):
        self.path = path

    def _set_if_not_set(self, key, value):
        checked = collect_output(
            ['git', 'config', '--global', key],
            check=False
        )
        if not checked.strip():
            run_command(['git', 'config', '--global', key, value])

    def clone_branch(self, branch: str):
        """Clone the given branch into path. Initialize the dbt user so commits
        work.
        """
        if self.path.exists():
            shutil.rmtree(self.path)
        self.path.mkdir(parents=True)
        cmd = [
            'git', 'clone',
            '--single-branch', '--depth=1',
            '--branch', branch, DBT_REPO,
            str(self.path)
        ]
        run_command(cmd)

    def checkout_branch(self, branch: str):
        cmd = ['git', 'checkout', branch]
        run_command(cmd, cwd=self.path)
        self._set_if_not_set('user.email', 'circleci@fishtownanalytics.com')
        self._set_if_not_set('user.name', 'CircleCI Build Bot')

    def _rev_parse(self, commitish: str) -> str:
        cmd = ['git', 'rev-parse', commitish]
        return collect_output(cmd, cwd=self.path).strip()

    def ensure_matching_commits(self, release: ReleaseFile):
        commit_result = self._rev_parse(release.commit)
        branch_result = self._rev_parse(release.branch)
        if commit_result != branch_result:
            raise ValueError(
                f'Commit {release.commit} points to sha {commit_result}, '
                f'but branch {release.branch} points to sha {branch_result}'
            )
        if not commit_result.startswith(release.commit):
            raise ValueError(
                f'Sha {commit_result} does not start with {release.commit}'
            )

    def set_version(self, release: ReleaseFile, env_path: Path):
        bin_path = env_path / 'bin/bumpversion'
        # if we always set the version to major, this appears to always work...
        cmd = [
            bin_path.absolute(), '--commit', '--no-tag', '--new-version',
            str(release.version), 'major'
        ]
        print(f'bumping version to {release.version}')
        run_command(cmd, cwd=self.path)
        print(f'bumped version to {release.version}')


class PypiBuilder:
    _SUBPACKAGES = (
        'core',
        'plugins/postgres',
        'plugins/redshift',
        'plugins/bigquery',
        'plugins/snowflake',
    )

    def __init__(self, dbt_path: Path, pkg_env_path: Path):
        self.dbt_path = dbt_path
        self.pkg_env_path = pkg_env_path.absolute()

    @staticmethod
    def _dist_for(path: Path, make=False) -> Path:
        dist_path = path / 'dist'
        if dist_path.exists():
            shutil.rmtree(dist_path)
        if make:
            dist_path.mkdir(parents=True, exist_ok=True)
        build_path = path / 'build'
        if build_path.exists():
            shutil.rmtree(build_path)
        return dist_path

    def _build_pypi_package(self, path: Path):
        print(f'building package in {path}')
        env_python = self.pkg_env_path / 'bin' / 'python'
        cmd = [env_python, 'setup.py', 'sdist', 'bdist_wheel']
        run_command(cmd, cwd=path)
        print(f'finished building package in {path}')

    @staticmethod
    def _all_packages_in(path: Path) -> Iterator[Path]:
        path = path / 'dist'
        for pattern in ('*.tar.gz', '*.whl'):
            yield from path.glob(pattern)

    def built_packages(self) -> Iterator[Path]:
        return self._all_packages_in(self.dbt_path)

    def built_wheels(self) -> List[Path]:
        dist_path = self.dbt_path / 'dist'
        return [p.absolute() for p in dist_path.glob('*.whl')]

    def _build_subpackage(self, name: str) -> Iterator[Path]:
        subpath = self.dbt_path / name
        self._dist_for(subpath)
        self._build_pypi_package(subpath)
        return self._all_packages_in(subpath)

    def build(self):
        print('building pypi packages')
        dist_path = self._dist_for(self.dbt_path)
        sub_pkgs: List[Path] = []
        for path in self._SUBPACKAGES:
            sub_pkgs.extend(self._build_subpackage(path))

        # now build the main package
        self._build_pypi_package(self.dbt_path)
        # now copy everything from the subpackages in
        for package in sub_pkgs:
            shutil.copy(str(package), dist_path)

        print('built pypi packages')

    def setup_test_env(self, env_path: Path):
        wheels = self.built_wheels()
        if not (self.dbt_path / 'test/integration').exists():
            raise ValueError('No tests???')
        install_env = DbtWheelEnv(self.dbt_path, wheels)
        print('Setting up virtualenv for tests')
        install_env.create(env_path)
        print('Test virtualenv created')
        return env_path

    def test(self, env_path: Path, name: str):
        self.setup_test_env(env_path)
        test_python_path = (env_path / 'bin/python').absolute()

        if name == 'rpc':
            # RPC tests first
            cmd = [
                test_python_path, '-m', 'pytest',
                '--durations', '0', '-v', '-n4',
                'test/rpc/'
            ]
            startmsg = 'Running RPC tests'
            endmsg = 'RPC tests passed'
        else:
            cmd = [
                test_python_path, '-m', 'pytest',
                '--durations', '0', '-v', '-m', f'profile_{name}', '-n4',
                'test/integration/'
            ]
            startmsg = f'Running tests for plugin: {name}'
            stream_output(cmd, cwd=self.dbt_path)
            endmsg = f'Tests for plugin: {name} passed'

        print(startmsg)
        stream_output(cmd, cwd=self.dbt_path)
        print(endmsg)

    def upload(self, *, test=True):
        cmd = ['twine', 'check']
        cmd.extend(str(p) for p in self.built_packages())
        run_command(cmd)
        cmd = ['twine', 'upload']
        if test:
            cmd.extend(['--repository', 'pypitest'])
        cmd.extend(str(p) for p in self.built_packages())
        print('uploading packages: {}'.format(' '.join(cmd)))
        run_command(cmd)
        print('uploaded packages')


class PipInstaller(venv.EnvBuilder):
    def __init__(self, packages: List[str]) -> None:
        super().__init__(with_pip=True)
        self.packages = packages

    def post_setup(self, context):
        # we can't run from the dbt directory or this gets all weird, so
        # install from an empty temp directory and then remove it.
        with tempfile.TemporaryDirectory() as tmp:
            cmd = [context.env_exe, '-m', 'pip', 'install', '--upgrade']
            cmd.extend(self.packages)
            print(f'installing {self.packages}')
            run_command(cmd, cwd=tmp)
        print(f'finished installing {self.packages}')

    def create(self, venv_path: Path):
        venv_path.parent.mkdir(parents=True, exist_ok=True)
        if venv_path.exists():
            shutil.rmtree(venv_path)
        return super().create(venv_path)


def _require_wheels(dbt_path: Path) -> List[Path]:
    dist_path = dbt_path / 'dist'
    wheels = list(dist_path.glob('*.whl'))
    if not wheels:
        raise ValueError(
            f'No wheels found in {dist_path} - run scripts/build-wheels.sh'
        )
    return wheels


class EnvBuilder(venv.EnvBuilder):
    def dbt_pip_install(self, cwd, context, *pkgs, upgrade=True):
        cmd = [context.env_exe, '-m', 'pip', 'install']
        if upgrade:
            cmd.append('--upgrade')
        cmd.extend(pkgs)
        run_command(cmd, cwd=cwd)

    def create(self, venv_path: Path):
        venv_path.parent.mkdir(parents=True, exist_ok=True)
        if venv_path.exists():
            shutil.rmtree(venv_path)
        return super().create(venv_path)


class DbtWheelEnv(EnvBuilder):
    def __init__(self, dbt_path: Path, wheels: List[Path]) -> None:
        super().__init__(with_pip=True)
        if not wheels:
            raise ValueError(
                f'No wheels found!'
            )
        self.dbt_path = dbt_path.absolute()
        self.wheels = wheels

    def _get_wheel_install_order(self) -> List[List[Path]]:
        core = []
        plugins = []
        final = []
        for whl in self.wheels:
            if whl.name.startswith('dbt-'):
                final.append(whl)
            elif whl.name.startswith('dbt_core-'):
                core.append(whl)
            else:
                plugins.append(whl)

        if len(core) == 0:
            raise ValueError(
                f'Could not find wheel for dbt-core in {self.wheels}'
            )
        if len(final) == 0:
            raise ValueError(f'Could not find wheel for dbt in {self.wheels}')
        if len(plugins) != 4:
            raise ValueError(
                f'Expected 4 plugins, got {len(plugins)}: {plugins}'
            )
        return [core, plugins, final]

    def post_setup(self, context):
        with tempfile.TemporaryDirectory() as tmp:
            for pkglist in self._get_wheel_install_order():
                self.dbt_pip_install(tmp, context, *pkglist)

            dev_reqs_path = str(self.dbt_path / 'dev_requirements.txt')
            self.dbt_pip_install(tmp, context, '-r', dev_reqs_path)


class PackagingEnv(EnvBuilder):
    def __init__(self):
        super().__init__(with_pip=True)

    def post_setup(self, context):
        with tempfile.TemporaryDirectory() as tmp:
            self.dbt_pip_install(
                tmp, context,
                'wheel', 'setuptools', 'virtualenv==20.0.3',
                'bumpversion', 'twine',
            )


@contextmanager
def moved_directory(path: Path):
    oldcwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldcwd)


def build_wheels(args=None):
    release = ReleaseFile.from_git()
    build_dir = Path('./build')
    pkg_dir = build_dir / 'pkg_venv'
    pkgenv = PackagingEnv()
    pkgenv.create(pkg_dir)

    dbt_path = build_dir / 'dbt'
    repository = DbtRepository(dbt_path)
    repository.checkout_branch(release.branch)
    repository.ensure_matching_commits(release)
    repository.set_version(release, pkg_dir)
    pypi_builder = PypiBuilder(dbt_path, pkg_dir)
    pypi_builder.build()


def test_wheels(args=None):
    if args is None:
        target = 'postgres'
    else:
        target = args.test_name

    build_dir = Path('./build')
    dbt_path = build_dir / 'dbt'
    pypi_builder = PypiBuilder(dbt_path, build_dir / 'pkg_venv')
    pypi_builder.test(build_dir / 'test_venv', target)


def store_artifacts(args=None):
    release = ReleaseFile.from_git()
    build_dir = Path('./build')
    dbt_path = build_dir / 'dbt'
    pypi_builder = PypiBuilder(dbt_path)

    artifacts_dir = Path('./artifacts')
    artifact_dist_dir = artifacts_dir / 'dist'
    artifact_dist_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(release.path, artifacts_dir / 'release.txt')
    for path in pypi_builder.built_packages():
        shutil.copyfile(path, artifact_dist_dir / path.name)

    # this will fail, because no pypi credentials
    # if args is None or args.do_test_upload:
    #     pypi_builder.upload(test=True)
    # pypi_builder.upload(test=False)


def parse_args():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers(title="Available sub-commands")

    pkg_sub = subs.add_parser('package', help='build the wheels/tarfiles')
    pkg_sub.set_defaults(func=build_wheels)

    test_sub = subs.add_parser('test', help='Run the given tests')
    test_sub.add_argument(
        'test_name',
        choices=['rpc', 'postgres', 'redshift', 'bigquery', 'snowflake'],
    )
    test_sub.set_defaults(func=test_wheels)

    upload_sub = subs.add_parser('upload', help='Upload the package to pypi')
    upload_sub.add_argument(
        '--skip-test-upload',
        help='Skip uploading to testpypi',
        action='store_false',
        dest='do_test_upload',
    )
    upload_sub.set_defaults(func=store_artifacts)

    return parser.parse_args()


if __name__ == '__main__':
    # avoid "what's a bdist_wheel" errors
    try:
        import wheel  # type: ignore # noqa
    except ImportError:
        print(
            'The wheel package is required to build. Please run:\n'
            'pip install -r dev_requirements.txt'
        )
        sys.exit(1)

    parsed = parse_args()
    if not hasattr(parsed, 'func'):
        print('No arguments passed!')
        sys.exit(2)
    parsed.func(parsed)
