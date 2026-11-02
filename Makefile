SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
# Git Bash on Windows would rewrite container paths such as /var/lib/... into Windows paths.
export MSYS_NO_PATHCONV := 1

.PHONY: help install lint fmt test test-fast test-data data d2-install diagrams diagrams-check diagrams-watch

# Scale factor of the TPC-DS data: 0.01 (CI), 1 (default demo), 10 (documented heavy run).
SF ?= 1

help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

install: ## Install dependencies
	uv sync --locked

lint: ## Ruff check, ruff format check, mypy (the CI lint job)
	uv run ruff check
	uv run ruff format --check
	uv run mypy

fmt: ## Format and apply safe lint fixes
	uv run ruff format
	uv run ruff check --fix

test-fast: ## Unit tests, no data and no stack
	uv run pytest

test-data: ## Generate and build TPC-DS SF0.01 in a temp dir and check it (marker slow)
	uv run pytest -m slow

test: test-fast test-data ## Everything that runs without the stack

data: ## Generate TPC-DS and build the OLTP seed at scale SF (make data SF=0.01)
	uv run retail-pricing data all --sf $(SF)

# Diagrams: docs/diagrams/<name>.d2 rendered to <name>.svg (committed). One pinned D2 everywhere.
# Layout engine, themes and padding live in docs/diagrams/_style.d2 (d2-config), so d2 runs in a
# clean environment: D2_* and SCALE variables would override that config. --scale 1 is not a
# d2-config key; it writes the natural width and height, so GitHub never upscales a small diagram.
D2_VERSION := 0.9.0
# sha256 of d2-v$(D2_VERSION)-linux-amd64.tar.gz, from the release's SHA256SUMS
D2_SHA256 := 5669ddc46b99e942cc96078f4a4e36d5e62103348f4c05179ede27802fdd87a9
D2_HOME ?= $(HOME)/.local/share/d2/v$(D2_VERSION)
D2 ?= $(D2_HOME)/bin/d2
D2_RUN = env -i PATH="$$PATH" HOME="$$HOME" "$(D2)" --scale 1
DIAGRAMS := $(filter-out docs/diagrams/_%,$(wildcard docs/diagrams/*.d2))

d2-install: ## Install the pinned D2 (Linux x86_64) into D2_HOME, checksum verified
	@if [ -x "$(D2_HOME)/bin/d2" ]; then echo "D2 v$(D2_VERSION) already in $(D2_HOME)"; exit 0; fi; \
	[ "$$(uname -sm)" = "Linux x86_64" ] || { echo "d2-install covers Linux x86_64 only;" \
		"install D2 v$(D2_VERSION) yourself and pass D2=/path/to/d2" >&2; exit 1; }; \
	tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	curl -fsSLo "$$tmp/d2.tar.gz" \
		"https://github.com/terrastruct/d2/releases/download/v$(D2_VERSION)/d2-v$(D2_VERSION)-linux-amd64.tar.gz"; \
	echo "$(D2_SHA256)  $$tmp/d2.tar.gz" | sha256sum --check --strict; \
	tar -xzf "$$tmp/d2.tar.gz" -C "$$tmp"; \
	mkdir -p "$(D2_HOME)/bin"; \
	install -m 0755 "$$tmp/d2-v$(D2_VERSION)/bin/d2" "$(D2_HOME)/bin/d2"; \
	"$(D2_HOME)/bin/d2" --version

diagrams: ## Render docs/diagrams/*.d2 to SVG with the pinned D2 (make d2-install once)
	@v="$$($(D2_RUN) --version 2>/dev/null || true)"; [ "$$v" = "v$(D2_VERSION)" ] || { \
		echo "need D2 v$(D2_VERSION) at $(D2), found '$${v:-nothing}': run make d2-install" >&2; exit 1; }
	@for f in $(DIAGRAMS); do $(D2_RUN) "$$f" "$${f%.d2}.svg"; done

diagrams-check: diagrams ## Render, then fail if a committed SVG is stale, missing or has no source
	@shopt -s nullglob; for s in docs/diagrams/*.svg; do \
		[ -f "$${s%.svg}.d2" ] || { echo "$$s has no .d2 source" >&2; exit 1; }; done
	@changed="$$(git status --porcelain --untracked-files=all -- 'docs/diagrams/*.svg')"; \
	if [ -n "$$changed" ]; then echo "$$changed" >&2; \
		echo "rendered SVGs differ from the committed ones: run make diagrams and commit" >&2; exit 1; fi

diagrams-watch: ## Live preview in the browser while editing: make diagrams-watch NAME=architecture
	@# NAME only from the command line: WSL exports NAME (the hostname) into every shell.
	@[ "$(origin NAME)" = "command line" ] && [ -f "docs/diagrams/$(NAME).d2" ] || { \
		echo "usage: make diagrams-watch NAME=<one of: $(basename $(notdir $(DIAGRAMS)))>" >&2; exit 1; }
	$(D2_RUN) --watch --browser "$${BROWSER:-0}" docs/diagrams/$(NAME).d2 docs/diagrams/$(NAME).svg
