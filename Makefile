up:
	docker compose -f docker-compose.flink.yaml up -d
	docker compose -f docker-compose.kafka.yaml up -d
	docker compose -f docker-compose.lakehouse.yaml up -d
	docker compose -f docker-compose.postgres.yaml up -d

setup_source:
	uv run python -m pipeline.datasource.setup_datasource

enrich:
	docker exec flink-jobmanager flink run -py ./flink_jobs/jobs/enrichment_job.py

down:
	docker compose -f docker-compose.flink.yaml down -v
	docker compose -f docker-compose.kafka.yaml down -v
	docker compose -f docker-compose.lakehouse.yaml down -v
	docker compose -f docker-compose.postgres.yaml down -v
