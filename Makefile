.DEFAULT_GOAL = help
.PHONY: lint lint-fix format format-check deps-audit build release upgrade deny-check test test-redis run watch

##
## Linters:
lint: ## - Run lint for checking code style
	cargo clippy --all-targets -- -Dwarnings

lint-fix: ## - Run lint for auto-fix code style
	cargo fix

format: ## - rustfmt
	cargo fmt

format-check: ## - rustfmt check
	cargo fmt --check

deps-audit: ## - Audit dependencies with cargo-audit
	cargo audit

##
## Run:
build: ## - Build
	cargo build

release: ## - Build release
	cargo build --release

upgrade: ## - Updating versions to the latest version in the Cargo.toml with cargo-edit
	cargo upgrade

deny-check: ## - Check dependencies with cargo-deny
	cargo deny check

##
## Test:
test: ## - tests
	cargo test

test-redis: ## - Isolated Redis integration test (requires redis-server)
	cargo test --test redis_snapshot -- --ignored

run: ## - Run with config.yaml
	cargo run --release

watch: ## - Poll Redis snapshots and print external observations
	cargo run --release --locked --bin snapshot-watch

##
## Other:
help: ## - Show help message
	@grep -F -h "##" $(MAKEFILE_LIST) | grep -F -v grep -F | sed -e 's/\\$$//' | awk 'BEGIN {FS = ":*[[:space:]]*##[[:space:]]*"}; \
	{ \
		if($$2 == "") \
			printf ""; \
		else if($$0 ~ /^#/) \
			printf "\n%s\n", $$2; \
		else if($$1 == "") \
			printf "     %-20s%s\n", "", $$2; \
		else \
			printf "\n    \033[34m%-20s\033[0m %s\n", $$1, $$2; \
	}'
	@echo -e ""
