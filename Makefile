.PHONY: install install_dev

install:
	pip install .

install_dev:
	pip install -e '.[dev]'

run_cleaning:
	python life_expectancy/main.py
