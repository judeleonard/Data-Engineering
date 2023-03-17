SHELL = /bin/bash

# Environment
.PHONY: setup
setup:
	python3 -m venv ~/.dbt && \
	source ~/.dbt/bin/activate && \
	pip3 install -r requirements.txt 

# Clean
.PHONY: clean
clean: 
	find . -type f -name "*.DS_Store" -ls -delete
	find . | grep -E "(__pycache__|\.pyc|\.pyo)" | xargs rm -rf
	find . | grep -E ".pytest_cache" | xargs rm -rf
	find . | grep -E ".ipynb_checkpoints" | xargs rm -rf
	find . | grep -E ".trash" | xargs rm -rf
	rm -f .coverage

# initialize dbt project in your directory
.PHONY: dbt
dbt: 
	dbt init

.PHONY: run
run: 
	dbt run

.PHONY: test
test: 
	dbt test

.PHONY: docs
docs: 
	dbt docs generate

.PHONY: serve
serve: 
	dbt docs serve

.PHONY: help
help:
	@echo "Commands:"
	@echo "setup    : creates a virtual environment (.dbt) for the project."
	@echo "clean    : deletes all unnecessary files and executes style formatting."
	@echo "init		: initialize dbt project in your current directory."
	@echo "run   	: starts running dbt"
	@echo "test		: test dbt model macros"
	@echo "docs	: generate documentation for the project."
	@echo "serve	: deploy project documentation to web server"