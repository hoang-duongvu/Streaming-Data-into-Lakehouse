up:
	docker compose -f docker_compose/docker-compose.flink.yaml up --scale flink-taskmanager=4 -d
	docker compose -f docker_compose/docker-compose.kafka.yaml up -d
	docker compose -f docker_compose/docker-compose.lakehouse.yaml up -d
	docker compose -f docker_compose/docker-compose.postgres.yaml up -d
	docker compose -f docker_compose/docker-compose.trino.yaml up -d

setup_source:
	uv run python -m pipeline.datasource.setup_datasource

jobs:
	docker exec flink-jobmanager flink run -py ./flink_jobs/jobs/enrichment_job.py
	docker exec flink-jobmanager flink run -py ./flink_jobs/jobs/stream_join_job.py
	docker exec flink-jobmanager flink run -py ./flink_jobs/jobs/window_agg_job.py	
	docker exec flink-jobmanager flink run -py ./flink_jobs/jobs/session_agg_job.py	

down:
	docker compose -f docker_compose/docker-compose.flink.yaml down -v
	docker compose -f docker_compose/docker-compose.kafka.yaml down -v
	docker compose -f docker_compose/docker-compose.lakehouse.yaml down -v
	docker compose -f docker_compose/docker-compose.postgres.yaml down -v
	docker compose -f docker_compose/docker-compose.trino.yaml down -v
