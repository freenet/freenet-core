# ==========================================
# Freenet Ping Application Builder
# ==========================================

# Use bash shell
SHELL := /bin/bash

# Project Structure
# ------------------------------------------
PROJECT_ROOT    := $(shell pwd)
PING_CONTRACT  := $(PROJECT_ROOT)/contracts/ping
PING_APP       := $(PROJECT_ROOT)/app
BUILD_DIR      := $(PING_CONTRACT)/target


# Log Command with ANSI color removal
# ------------------------------------------
define LOG_CMD
1> >(stdbuf -o0 sed 's/\x1b\[[0-9;]*m//g' >> $(1)) 2>&1
endef

# Build Tools
# ------------------------------------------
CARGO          := cargo
FDEV           := fdev

# Ping Configuration
# ------------------------------------------
WS_PORT        ?= 3001     # WebSocket port for node connection
LOG_LEVEL      ?= debug     # Logging level
FREQUENCY      ?= 1000ms   # Update frequency with time unit
TTL            ?= 3600s    # Time to live with time unit

# PHONY Targets
# ------------------------------------------
.PHONY: help all verify build clean run install
.DEFAULT_GOAL := help

# Help Command
# ------------------------------------------
help:
	@echo "Freenet Ping Application Management"
	@echo ""
	@echo "Main Commands:"
	@echo "  make -f run-ping.mk build    - Build contract and application"
	@echo "  make -f run-ping.mk install  - Install freenet-ping tool"
	@echo "  make -f run-ping.mk run      - Run freenet-ping tool"
	@echo "  make -f run-ping.mk clean    - Clean build artifacts"
	@echo ""
	@echo "Configuration:"
	@echo "  WS_PORT=$(WS_PORT)     - WebSocket port for node connection"
	@echo "  FREQUENCY=$(FREQUENCY)  - Update frequency (e.g., 1000ms, 1s)"
	@echo "  TTL=$(TTL)             - Time to live (e.g., 3600s, 1h)"
	@echo ""
	@echo "Example:"
	@echo "  make -f run-ping.mk run WS_PORT=3002 FREQUENCY=2000ms TTL=7200s"

# Verification
# ------------------------------------------
verify:
	@echo "→ Verifying project structure..."
	@if [ ! -d "$(PING_CONTRACT)" ]; then \
		echo "Error: contracts/ping directory not found"; \
		echo "Please run from freenet-ping root directory"; \
		exit 1; \
	fi
	@if [ ! -d "$(PING_APP)" ]; then \
		echo "Error: app directory not found"; \
		echo "Please run from freenet-ping root directory"; \
		exit 1; \
	fi
	@echo "✓ Project structure verified"

# Build Commands
# ------------------------------------------
build: verify build-contract build-app

build-contract:
	@echo "→ Building ping contract..."
	@mkdir -p $(BUILD_DIR)
	@cd $(PING_CONTRACT) && CARGO_TARGET_DIR=$(BUILD_DIR) $(FDEV) build --features contract
	@echo "✓ Contract built successfully"

build-app:
	@echo "→ Building ping application..."
	@cd $(PING_APP) && $(CARGO) build
	@echo "✓ Application built successfully"

# Install Application
# ------------------------------------------
install: build
	@echo "→ Installing freenet-ping tool..."
	@cd $(PING_APP) && $(CARGO) install --path .
	@echo "✓ Tool installed successfully"

# Run Application
# ------------------------------------------
run: install
	@echo "→ Running freenet-ping..."
	@echo "  WebSocket Port: $(WS_PORT)"
	@echo "  Update Frequency: $(FREQUENCY)"
	@echo "  TTL: $(TTL)"
	@freenet-ping \
		--host "localhost:$(WS_PORT)" \
		--log-level $(LOG_LEVEL) \
		--frequency $(FREQUENCY) \
		--ttl $(TTL)

# Cleanup
# ------------------------------------------
clean:
	@echo "→ Cleaning build artifacts..."
	@cd $(PING_APP) && $(CARGO) clean
	@rm -rf $(BUILD_DIR)
	@echo "✓ Clean completed"