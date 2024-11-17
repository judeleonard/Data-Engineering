ASTRO_VERSION=v1.18.2

.PHONY: all
all: setup start

# checks and install Astro CLI if not found
.PHONY: setup
setup:
	@which astro > /dev/null 2>&1 || ( \
		echo "Astro CLI not found. Installing Astro CLI..."; \
		curl -sSL https://install.astronomer.io | sudo bash -s -- $(ASTRO_VERSION) \
	)
	@[ -d .astro ] || ( \
	echo ".astro directory not found. Initializing Astro project..."; \
	astro dev init \
	)
	@echo "Cleaning redundant files..."
	@find . -type f -name "*.DS_Store" -ls -delete
	@find . | grep -E "(__pycache__|\.pyc|\.pyo)" | xargs rm -rf
	@find . | grep -E ".pytest_cache" | xargs rm -rf
	@find . | grep -E ".ipynb_checkpoints" | xargs rm -rf
	@find . | grep -E ".trash" | xargs rm -rf
	@rm -f  ./dags/example_dag_basic.py ./dags/example_dag_advanced.py

# launch Airflow with Astro
.PHONY: start
start:
	astro dev start

# ci/cd workflow
.PHONY: update
update:
	astro dev restart

# Clean up
.PHONY: clean
clean:
	astro dev kill

# Clean up
.PHONY: stop
stop:
	astro dev stop

# run pytest
.PHONY: test
test:
	astro dev pytest

.PHONY: help
help:
	@echo "Commands:"
	@echo "setup  : install astro runtime if not found and initiate the project as well as removing unnecessary files."
	@echo "clean  : tear down and remove all running process including all configurations set in the metastore"
	@echo "stop  :  stops all running process but will retain all configurations set in the metastore"
	@echo "update : this commands updates the changes made to your pipeline"
	@echo "start  : starts running all containers neccessary to run airflow ."
	@echo "test   : This command will run pytest if any is available in the test directory."
