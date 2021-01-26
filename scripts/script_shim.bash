#!/bin/bash

HERE="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHONPATH="$HERE/release-pypath" python -m builder $@
