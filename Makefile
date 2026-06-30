# QTP — one-command targets for the demo + compliance cadence.
# Cross-platform convenience (Linux/macOS graders); Windows users can run ./scripts/demo.ps1.

.PHONY: help up down demo demo-live preflight schedule schedule-install evidence seed clean

help:           ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-18s %s\n", $$1, $$2}'

up:             ## Start the whole stack (single command, with health checks)
	docker compose up -d

down:           ## Stop the stack (keep volumes)
	docker compose down

demo: up        ## Bring up the stack and run the end-to-end compliance demonstration
	qtp preflight run
	qtp demo run

demo-live: up   ## Demo with live overlays (inject a real CRITICAL into Kafka)
	qtp demo run --inject-critical

preflight:      ## Print the service health board
	qtp preflight run

seed:           ## Seed only the golden-thread scenario
	qtp demo seed

schedule:       ## Run the recurring compliance cadence in this process
	qtp schedule run

schedule-install: ## Write cron / Task Scheduler / compose scheduler artefacts
	qtp schedule install

evidence:       ## Assemble a compliance evidence pack from current warehouse state
	qtp evidence pack

clean:          ## Stop the stack and remove volumes (full reset)
	docker compose down -v
