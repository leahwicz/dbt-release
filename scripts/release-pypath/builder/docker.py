from pathlib import Path
from .cmd import stream_output
from .common import EnvironmentInformation, ReleaseFile


def build_docker(args=None):
    env = EnvironmentInformation()
    release = ReleaseFile.from_artifacts(env)
    requirements_path = env.get_dbt_requirements_file(str(release.version))
    if requirements_path == requirements_path.absolute():
        requirements_path = requirements_path.relative_to(Path.cwd())

    remote_tag = f'fishtownanalytics/dbt{release.version}'

    cmd = [
        'docker', 'build',
        '--no-cache',
        '--build-arg',
        f'BASE_REQUIREMENTS_SRC_PATH={requirements_path}',
        '--build-arg',
        'DIST_PATH=./artifacts/dist/',
        '--build-arg',
        'WHEEL_REQUIREMENTS_SRC_PATH=./artifacts/wheel_requirements.txt',
        '--tag', remote_tag,
        '.'
    ]

    stream_output(cmd)

    cmd = [
        'docker', 'push', remote_tag
    ]


def add_docker_parsers(subparsers):
    docker_sub = subparsers.add_parser(
        'docker', help='Build and push the docker image'
    )
    docker_subs = docker_sub.add_subparsers(title='Available sub-commands')
    build_sub = docker_subs.add_parser('build', help='build the docker image')
    build_sub.set_defaults(func=build_docker)
