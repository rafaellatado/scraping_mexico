.PHONY: all again new clean remove create install

VENV_DIR := .venv
SYSTEM_PYTHON := python3
PYTHON := $(VENV_DIR)/bin/python
PIP := $(VENV_DIR)/bin/pip

all: install
	@echo "Running main script..."
	@$(PYTHON) main.py

new:
	@$(PIP) freeze > requirements.txt
	@echo "Requirements file updated"

clean:
	@echo "Cleaning up..."
	@rm -rf core/__pycache__
	@rm -rf csv/tmp csv/finalCSV
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Clean complete"

remove: clean
	@echo "Removing virtual environment and data..."
	@rm -rf $(VENV_DIR)
	@rm -rf csv/
	@echo "Remove complete"

create:
	@if [ -d "$(VENV_DIR)" ]; then \
		echo "Virtual environment already exists at $(VENV_DIR)"; \
	else \
		echo "Creating virtual environment..."; \
		$(SYSTEM_PYTHON) -m venv $(VENV_DIR) && \
		echo "Virtual environment created at $(VENV_DIR)"; \
	fi

install: create
	@echo "Installing requirements..."
	@$(PIP) install -r requirements.txt