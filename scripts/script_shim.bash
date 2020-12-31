#!/bin/bash

HERE="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_AUTHOR_NAME=Github Build Bot
GIT_AUTHOR_EMAIL=buildbot@fishtownanalytics.com
PYTHONPATH="$HERE/release-pypath" python -m builder $@
