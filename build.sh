#!/bin/zsh
export PATH="/usr/bin:/bin:/usr/sbin:/usr/local/bin"
set -o errexit
set -o nounset

readonly PYTHON="/usr/local/bin/python3"
readonly ENV=".venv"
readonly ACTIVATE="${ENV}/bin/activate"

if [[ -e "${ENV}" ]]; then
    echo "Found old virtual environment folder. Deleting"
     rm -rf "${ENV}"
fi

echo "Activate venv and install wheels"
${PYTHON} -m venv ${ENV}
source ${ACTIVATE}

echo "Updating and installing requirements"
python -m pip install pip wheel setuptools --upgrade
pip install -r "requirements.txt" --upgrade
echo "Using:"
python --version
pip --version

deactivate
