.PHONY: all build jar test check checkstyle coverage clean docs docs-serve

all: check build

build jar:
	./gradlew build shadowJar

test:
	./gradlew test

check:
	./gradlew check

checkstyle:
	./gradlew checkstyleMain checkstyleTest

coverage:
	./gradlew test integrationTest jacocoTestReport jacocoTestCoverageVerification
	@echo "Report: build/reports/jacoco/test/html/index.html"

clean:
	./gradlew clean

# Docs. The site needs mkdocs-material from docs/requirements.txt; a globally installed mkdocs
# does not have it and fails with "Unrecognised theme name: 'material'". These targets keep a
# private virtualenv in .venv and always run the mkdocs inside it.
VENV := .venv
DOCS_STAMP := $(VENV)/.docs-requirements

$(DOCS_STAMP): docs/requirements.txt
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --quiet -r docs/requirements.txt
	@touch $@

docs: $(DOCS_STAMP)
	cd docs && ../$(VENV)/bin/mkdocs build --strict --site-dir ../build/docs-site

docs-serve: $(DOCS_STAMP)
	cd docs && ../$(VENV)/bin/mkdocs serve
