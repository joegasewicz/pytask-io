redis:
	docker run -d -p 6379:6379 redis

test:
	pipenv run pytest -vvv