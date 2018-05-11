#   Makefile
#
# license   http://opensource.org/licenses/MIT The MIT License (MIT)
# copyright Copyright (c) 2018, Jeff Tanner, jeff00seattle
#

.PHONY: clean version build dist local-dev yapf pyflakes pylint

PACKAGE := FacebookAdsWorker
PACKAGE_NAME := facebook_ads

TMP_RESULT_GZIP_FILE="tmp/$(PACKAGE_NAME).json.gz"

PYTHON3 := $(shell which python3)
PIP3    := $(shell which pip3)
PYTHON3_VERSION := $(shell $(PYTHON3) --version)

PY_MODULES := pip setuptools pylint flake8 pprintpp pep8 requests six sphinx wheel python-dateutil

PACKAGE_SUFFIX := py3-none-any.whl
PACKAGE_WILDCARD := $(PACKAGE)-*
PACKAGE_PREFIX_WILDCARD := $(PACKAGE_PREFIX)-*
PACKAGE_PATTERN := $(PACKAGE_PREFIX)-*-$(PACKAGE_SUFFIX)

WHEEL_ARCHIVE := dist/$(PACKAGE_PREFIX)-$(VERSION)-$(PACKAGE_SUFFIX)

PACKAGE_ALL_FILES := $(shell find . -type f -name "*.py")
PYFLAKES_ALL_FILES := $(shell find . -type f  -name '*.py' ! '(' -name '__init__.py' ')')

REQ_FILE := requirements.txt
TOOLS_REQ_FILE := requirements-tools.txt
ALL_FILES := $(PACKAGE_FILES) $(REQ_FILE) $(SETUP_FILE)

clean:
	@echo "======================================================"
	@echo clean
	@echo "======================================================"
	@rm -fR __pycache__
	@rm -fR *.pyc
	@rm -fR tmp
	@rm -fR *.zip

install: clean
	@echo "======================================================"
	@echo install
	@echo "======================================================"
	$(PIP3) install --upgrade pip
	$(PIP3) install --upgrade -r requirements.txt

requirements: $(REQ_FILE)
	@echo "======================================================"
	@echo requirements
	@echo "======================================================"
	$(PYTHON3) -m pip install --upgrade -r $(REQ_FILE)

requirements-tools: $(TOOLS_REQ_FILE)
	@echo "======================================================"
	@echo requirements-tools
	@echo "======================================================"
	$(PYTHON3) -m pip install --upgrade -r $(TOOLS_REQ_FILE)

pep8: requirements-tools
	@echo "======================================================"
	@echo pep8 $(PACKAGE)
	@echo "======================================================"
	$(PYTHON3) -m pep8 --config .pep8 $(PACKAGE_ALL_FILES)

pyflakes: requirements-tools
	@echo "======================================================"
	@echo pyflakes $(PACKAGE)
	@echo "======================================================"
	$(PYTHON3) -m pip install --upgrade pyflakes
	$(PYTHON3) -m pyflakes $(PYFLAKES_ALL_FILES)

pylint: requirements-tools
	@echo "======================================================"
	@echo pylint $(PACKAGE)
	@echo "======================================================"
	$(PYTHON3) -m pip install --upgrade pylint
	$(PYTHON3) -m pylint --rcfile .pylintrc $(PACKAGE_ALL_FILES) --disable=C0330,F0401,E0611,E0602,R0903,C0103,E1121,R0913,R0902,R0914,R0912,W1202,R0915,C0302 | more -30

yapf: requirements-tools
	@echo "======================================================"
	@echo yapf $(PACKAGE)
	@echo "======================================================"
	$(PYTHON3) -m yapf --style .style.yapf --in-place $(PACKAGE_ALL_FILES)

lint: requirements-tools
	@echo "======================================================"
	@echo lint $(PACKAGE)
	@echo "======================================================"
	pylint --rcfile .pylintrc $(REQUESTS_MV_INTGS_FILES) | more

flake8:
	@echo "======================================================"
	@echo flake8 $(PACKAGE)
	@echo "======================================================"
	flake8 --ignore=F401,E265,E129 $(PACKAGE_PREFIX)

#
# Local run worker.py with requirements install.
#
local-run: install
	@echo $(PYTHON3): $(PYTHON3_VERSION)
	@echo "======================================================"
	@echo local-run
	@echo "======================================================"
	@$(PYTHON3) worker.py || (echo "local-run failed $$?"; exit 1)

install-tools:
	@echo "======================================================"
	@echo install-tools
	@echo "======================================================"
	$(PIP3) install --upgrade pip
	$(PIP3) install --upgrade -r requirements-tools.txt

results: install-tools
	@echo "======================================================"
	@echo results $(TMP_RESULT_GZIP_FILE)
	@echo "======================================================"
	@test -f $(TMP_RESULT_GZIP_FILE) || (echo "File $(TMP_RESULT_GZIP_FILE) missing!" ; exit 1)
	@ls -al tmp/*.gz
	@mkdir -p tmp/reports
	@rm -fR tmp/reports/*.csv
	@rm -fR tmp/reports/*.html
	@rm -fR tmp/reports/*.tex
	@rm -fR tmp/reports/*.gz
	@$(PYTHON3) parser/results_parser.py --data $(TMP_RESULT_GZIP_FILE) --csv --html
	@ls -al tmp/reports/*.csv 2>/dev/null
	@ls -al tmp/reports/*.html 2>/dev/null
	@ls -al tmp/reports/*.tex 2>/dev/null
	@ls -al tmp/reports/*.gz 2>/dev/null

list:
	cat Makefile | grep "^[a-z]" | awk '{print $$1}' | sed "s/://g" | sort
