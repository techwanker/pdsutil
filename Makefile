init:
	pip3 install -r requirements.text

test:
	py.test tests

.PHONY: init test
