.ONESHELL:

setup:
	python3 -m venv venv
	. venv/bin/activate
	pip3 install -r requirements.txt

format:
	. venv/bin/activate
	isort *.py
	black .
