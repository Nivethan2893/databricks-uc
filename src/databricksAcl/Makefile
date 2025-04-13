.PHONY: install test clean build

install:
	pip install -r requirements.txt

test:
	pytest tests/

clean:
	rm -rf dist build *.egg-info

build: clean
	python -m build