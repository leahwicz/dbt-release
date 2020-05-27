from pathlib import Path
from typing import Optional, List
import re
import shutil
import tempfile
import venv
from .cmd import stream_output
from .common import PackageType, VERSION_PATTERN_STR


class EnvBuilder(venv.EnvBuilder):
    def dbt_pip_install(self, cwd, context, *pkgs, upgrade=True):
        cmd = [context.env_exe, '-m', 'pip', 'install']
        if upgrade:
            cmd.append('--upgrade')
        cmd.extend(pkgs)
        stream_output(cmd, cwd=cwd)

    def create(self, venv_path: Path):
        venv_path.parent.mkdir(parents=True, exist_ok=True)
        if venv_path.exists():
            shutil.rmtree(venv_path)
        return super().create(venv_path)


class DBTPackageEnv(EnvBuilder):
    def __init__(
        self,
        package_dir: Path,
        requirements: Optional[Path] = None,
        ext=PackageType.Wheel,
    ) -> None:
        super().__init__(with_pip=True)
        self.package_dir = package_dir
        self.packages = [p.absolute() for p in self.package_dir.glob(ext.glob)]
        if not self.packages:
            raise ValueError(
                f'No files matching {ext.glob} found in {self.package_dir}!'
            )
        self.requirements = requirements
        self.ext = ext

    @staticmethod
    def is_pkg_name_pattern(filename: str, ending: Optional[str]) -> bool:
        # wheels are '{name.replace('-', '_')}-{version}-py3-none-any' (for us)
        # sdists are '{name}-{version}.tar.gz'

        name = 'dbt'
        if filename.endswith(PackageType.Sdist.suffix):
            namesep = '-'
            search = r'^{name}-{version}\.tar\.gz$'
        elif filename.endswith(PackageType.Wheel.suffix):
            namesep = '_'
            search = r'^{name}-{version}-py3-none-any\.whl$'
        else:
            raise ValueError(f'Unknown suffix: {filename}')

        if ending is not None:
            name = f'{name}{namesep}{ending}'

        pat = search.format(name=name, version=VERSION_PATTERN_STR)
        return bool(re.match(pat, filename))

    def get_pkg_install_order(self) -> List[Path]:
        """This method is important regardless of installation method, because
        pip needs to install the dependency first, and find it already
        installed when it goes to install the dependent.
        """
        core = []
        plugins = []
        final = []
        for package in self.packages:
            # order matters for these if-statements!
            if self.is_pkg_name_pattern(package.name, r'core'):
                core.append(package)
            elif self.is_pkg_name_pattern(package.name, r'[\w\d-]+'):
                plugins.append(package)
            elif self.is_pkg_name_pattern(package.name, None):
                final.append(package)
            else:
                raise ValueError(f'Unmatched package: {package}')

        if len(core) == 0:
            raise ValueError(
                f'Could not find package for dbt-core in {self.packages}'
            )
        if len(final) == 0:
            raise ValueError(
                f'Could not find package for dbt in {self.packages}'
            )
        if len(plugins) != 4:
            raise ValueError(
                f'Expected 4 plugins, got {len(plugins)}: {plugins}'
            )
        # If you sort plugins() by name postgres is before redshift, so it
        # works
        plugins.sort(key=lambda p: p.name)
        return core + plugins + final

    def post_dbt_install(self, tmp_dir: str, context):
        pass

    def post_setup(self, context):
        with tempfile.TemporaryDirectory() as tmp:
            if self.requirements is not None:
                requirements = str(self.requirements)
                self.dbt_pip_install(tmp, context, '-r', requirements)
            pkglist = self.get_pkg_install_order()
            self.dbt_pip_install(tmp, context, *pkglist)

            self.post_dbt_install(tmp, context)


class DevelopmentWheelEnv(DBTPackageEnv):
    def __init__(
        self,
        package_dir: Path,
        requirements: Optional[Path],
        dev_requirements: Path,
    ) -> None:
        super().__init__(package_dir=package_dir, requirements=requirements)
        self.dev_requirements = dev_requirements.absolute()

    def post_dbt_install(self, tmp_dir: str, context):
        self.dbt_pip_install(
            tmp_dir, context, '-r', str(self.dev_requirements)
        )


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


class PipInstalledDbtEnv(EnvBuilder):
    def __init__(self, dbt_version: str):
        super().__init__(with_pip=True)
        self.dbt_version = dbt_version

    def post_setup(self, context):
        with tempfile.TemporaryDirectory() as tmp:
            self.dbt_pip_install(tmp, context, f'dbt=={self.dbt_version}')
