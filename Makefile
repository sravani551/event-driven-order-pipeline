.PHONY: up up-ui scale down logs test lint load e2e dlq replay-dlq

up:            ## start everything
	docker compose up -d --build

up-ui:         ## start everything plus Kafka UI on :8080
	docker compose --profile ui up -d --build

scale:         ## 3 inventory consumers (one per partition) and 3 notification workers
	docker compose up -d --scale inventory=3 --scale notifier=3

down:
	docker compose --profile ui down -v

logs:
	docker compose logs -f inventory status-updater notifier

test:
	pytest -q

lint:
	ruff check .

load:          ## fire 200 orders at the API
	python scripts/load_test.py --orders 200

e2e:           ## place orders and assert every one reaches a final state
	python scripts/e2e_check.py

dlq:           ## list messages in the email dead-letter queue (non-destructive)
	python scripts/replay_email_dlq.py

replay-dlq:    ## move dead-lettered emails back to the work queue
	python scripts/replay_email_dlq.py --replay
