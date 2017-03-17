#/bin/bash
# This is a hack to work around https://github.com/tox-dev/tox/issues/42

pip install numpy && pip "$@"
