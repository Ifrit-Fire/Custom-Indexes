#!/bin/zsh
export PATH="/usr/bin:/bin:/usr/sbin:/usr/local/bin"
set -o errexit
set -o nounset

readonly ENV=".venv"
readonly ENV_PYTHON="${ENV}/bin/python"
readonly MAIN="src.main"

cd "${0:A:h}"

if [[ ! -x "${ENV_PYTHON}" ]]; then
    echo "Virtual environment not found at ${ENV}. Run build.sh first." >&2
    exit 1
fi

exec ${ENV_PYTHON} -m ${MAIN}
