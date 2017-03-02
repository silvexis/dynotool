.PHONY: help clean clean-pyc clean-build list test test-all coverage sdist

help:
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean - clean-{build,pyc}"
	@echo "lint - check style with flake8"
	@echo "test - run tests quickly with the default Python"
	@echo "testall - run tests on every Python version with tox"
	@echo "sdist - package"

clean: clean-build clean-pyc

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr *.egg-info

clean-pyc:
	find . -type f -name '*.py[co]' -exec rm -rf {} +
	find . -type d -name '__pycache__' -exec rm -rf {} +
	find . -name '*~' -exec rm -f {} +

lint:
	flake8 --ignore E265,E266,E402 --max-line-length=120

test:
	py.test

testall:
	tox

sdist: clean
	python setup.py sdist
	ls -l dist

bdist: clean
	python setup.py bdist_wheel

