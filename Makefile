.PHONY: up down build test gen worker tidy logs clean fmt

# --- local infra (Phase 0) ---
up:                       ## bring up redpanda, minio, prometheus, grafana + bootstrap topic/bucket
	docker compose up -d
	@echo "Kafka(host): localhost:19092  MinIO: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "Prometheus: http://localhost:9090  Grafana: http://localhost:3000"

down:                     ## tear down infra
	docker compose down -v

logs:
	docker compose logs -f --tail=100

# --- build & test ---
build:
	go build ./...

tidy:
	go mod tidy

fmt:
	go fmt ./...

test:                     ## run unit + integration tests (Phase 2 exit test lives here)
	go test ./...

# --- run the vertical slice locally ---
gen:                      ## produce a bounded dataset: make gen TOTAL=20000 EPS=2000 KEYS=100
	go run ./cmd/generator --eps=$(or $(EPS),2000) --keys=$(or $(KEYS),100) --total=$(or $(TOTAL),20000)

worker:                   ## run one standalone worker (owns all partitions/buckets, no coordinator)
	go run ./cmd/worker --id=worker-1 --window-size-ms=2000

coordinator:              ## run the coordinator: make coordinator WORKERS=3
	go run ./cmd/coordinator --workers=$(or $(WORKERS),1)

p3-test:                  ## Phase 3 exit test: 3-worker output == 1-worker output
	bash bench/p3_exit_test.sh

p4-test:                  ## Phase 4 exit test: checkpoints COMPLETED + worker restore
	bash bench/p4_checkpoint_test.sh

p5-test:                  ## Phase 5 exit test: kill worker mid-stream, recover, no data loss
	bash bench/p5_recovery_test.sh

p6-test:                  ## Phase 6 exit test: exactly-once staged commit + reconciliation under chaos
	bash bench/p6_reconciliation_test.sh

bench:                    ## Phase 7 benchmark: throughput/scaling + latency-vs-load -> bench/RESULTS.md
	bash bench/run_benchmark.sh

iceberg:                  ## Phase 8: append committed checkpoints to an Iceberg table + time-travel demo
	go run ./cmd/iceberg-sink

clean:
	rm -rf ./data ./bin
