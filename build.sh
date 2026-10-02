#!/bin/zsh
export PATH="/usr/bin:/bin:/usr/sbin:/usr/local/bin"
set -o errexit
set -o nounset

readonly PYTHON="/usr/local/bin/python3"
readonly ENV=".venv"
readonly ENV_PYTHON="${ENV}/bin/python"

echo "Creating fresh virtual environment"
${PYTHON} -m venv --clear ${ENV}

echo "Updating and installing requirements"
${ENV_PYTHON} -m pip install pip --upgrade
${ENV_PYTHON} -m pip install -r "requirements.txt" --upgrade
echo "Using:"
${ENV_PYTHON} --version
${ENV_PYTHON} -m pip --version
