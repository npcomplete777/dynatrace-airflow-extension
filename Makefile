VENV    ?= .venv
PYTHON  ?= $(VENV)/bin/python
PIP     ?= $(VENV)/bin/pip
DT_SDK  ?= $(VENV)/bin/dt-sdk
RUFF    ?= $(VENV)/bin/ruff
BUNDLE  ?= dist/custom_apache-airflow-1.0.0.zip

.PHONY: install lint format test fastcheck run gencerts build-bundle upload verify clean

install:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

lint:
	PATH="$(CURDIR)/$(VENV)/bin:$$PATH" $(RUFF) check .

format:
	PATH="$(CURDIR)/$(VENV)/bin:$$PATH" $(RUFF) format .

test:
	$(PYTHON) -m unittest discover -s tests -v

fastcheck:
	$(DT_SDK) run . --activation-config activation.json --secrets secrets.json --fastcheck

run:
	$(DT_SDK) run . --activation-config activation.json --secrets secrets.json

gencerts:
	mkdir -p certs
	$(DT_SDK) gencerts -o certs --ca_pass "" --dev_pass "" -f

## Build universal Linux x86_64 ActiveGate bundle (includes Python 3.10 & 3.14 wheels)
build-bundle:
	rm -rf extension/lib dist
	$(DT_SDK) wheel . -e manylinux2014_x86_64 -p 3.10 -o
	$(PYTHON) -m pip download -d extension/lib --only-binary=:all: --python-version 314 --platform manylinux2014_x86_64 charset_normalizer==3.5.2
	mkdir -p dist
	$(DT_SDK) assemble . -o dist/extension.zip
	$(DT_SDK) sign dist/extension.zip -c certs/developer.pem -o $(BUNDLE) -f
	rm -f dist/extension.zip

upload:
	@test -n "$(DT_ENV_URL)" || (echo "Set DT_ENV_URL=https://<ENV_ID>.apps.dynatrace.com" && exit 1)
	@test -n "$(DT_PLATFORM_TOKEN)" || (echo "Set DT_PLATFORM_TOKEN=dt0s16...." && exit 1)
	$(DT_SDK) upload $(BUNDLE) -u "$(DT_ENV_URL)" -p "$(DT_PLATFORM_TOKEN)" --active

verify:
	$(PYTHON) scripts/deploy_and_verify.py --verify-only

clean:
	rm -rf dist build extension/lib *.egg-info
