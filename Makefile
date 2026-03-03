check:
	ruff format --check .
	ruff check .

format:
	ruff format .
	ruff check --fix .

lint:
	ruff check .

.PHONY: check format lint
