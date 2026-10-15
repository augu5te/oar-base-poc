# OAR base system - developer entry points.
#
# The design requires the Cargo registry cache and index to live in the
# in-workspace `.cargo` directory, so every target below runs cargo with
# `CARGO_HOME=$(CURDIR)/.cargo` (the .cargo directory is git-ignored).
# Override it when you prefer the user-level cache:
#
#   make CARGO_HOME=$HOME/.cargo test

CARGO_HOME ?= $(CURDIR)/.cargo
export CARGO_HOME

CARGO ?= cargo

.DEFAULT_GOAL := help
.PHONY: help cargo-home build test test-unit test-e2e clippy fmt smoke clean distclean

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

cargo-home: ## Show which Cargo home (registry cache + index) is used
	@echo "CARGO_HOME=$(CARGO_HOME)"

build: ## Build every binary and library of the workspace
	$(CARGO) build --workspace

test: ## Run the whole test suite (unit + end to end)
	$(CARGO) test --workspace

test-unit: ## Run the unit tests only
	$(CARGO) test --workspace --lib

test-e2e: ## Run the in-process end-to-end tests only
	$(CARGO) test --workspace --test '*'

clippy: ## Lint every target (warnings are errors for the CI)
	$(CARGO) clippy --workspace --all-targets -- -D warnings

fmt: ## Format the workspace
	$(CARGO) fmt --all

smoke: build ## Live multi-process smoke test of the real daemons
	scripts/smoke_test.sh --no-build

clean: ## Remove the build artifacts
	$(CARGO) clean

distclean: clean ## Also remove the in-workspace cargo cache
	rm -rf "$(CURDIR)/.cargo"
