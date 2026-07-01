#!/bin/zsh
export PATH="/usr/bin:/bin:/usr/sbin:/usr/local/bin"
set -o errexit
set -o nounset

readonly PYTHON="python3"
readonly ENV=".venv"
readonly ACTIVATE="${ENV}/bin/activate"
readonly MAIN="./src/main.py"

if [[ ! -x "${ENV}/bin/${PYTHON}" ]]; then
    echo "Virtual environment not found at ${ENV}. Run build.sh first." >&2
    exit 1
fi

source "${ACTIVATE}"
exec ${PYTHON} ${MAIN}
