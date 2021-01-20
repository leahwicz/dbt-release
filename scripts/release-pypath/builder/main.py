#!/usr/bin/env python
import argparse
import sys

from .homebrew import add_homebrew_parsers
from .native import add_native_parsers
from .docker import add_docker_parsers
from .github import add_github_parsers


if sys.version_info < (3, 8):
    raise ValueError("Python 3.8 or greater required!")


def parse_args():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers(title="Available sub-commands")

    add_native_parsers(subs)
    add_homebrew_parsers(subs)
    add_docker_parsers(subs)
    add_github_parsers(subs)

    return parser.parse_args()


def main():
    parsed = parse_args()
    if not hasattr(parsed, "func"):
        print("No arguments passed!")
        sys.exit(2)
    parsed.func(parsed)
