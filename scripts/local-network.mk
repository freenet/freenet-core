# ==========================================
# Freenet Local Network Configuration
# ==========================================

SHELL := /bin/bash

# Directory Structure
# ------------------------------------------
HOME_DIR := $(HOME)
BASE_DIR := $(HOME_DIR)/.cache/freenet
KEYS_DIR := $(BASE_DIR)/keys
PID_DIR  := $(BASE_DIR)/pids
LOGS_DIR := $(BASE_DIR)/logs

# Network Configuration
# ------------------------------------------
N_NODES     := 1
N_GATEWAYS  := 1
BASE_PORT   := 3100
WS_BASE_PORT:= 3000

# Files
# ------------------------------------------
GW_CONFIG   := $(BASE_DIR)/gateways.toml

# Environment
# ------------------------------------------
ENV_VARS := RUST_BACKTRACE=1 RUST_LOG="info,freenet=debug,freenet-stdlib=debug,fdev=debug"
FREENET_CORE_PATH := ../crates/core/

# Log Command with ANSI color removal
# ------------------------------------------
define LOG_CMD
1> >(stdbuf -o0 sed 's/\x1b\[[0-9;]*m//g' >> $(1)) 2>&1
endef

# PHONY Targets
# ------------------------------------------
.PHONY: help setup start stop clean logs status
.DEFAULT_GOAL := help

# Help Command
# ------------------------------------------
help:
	@echo "Freenet Local Network Management"
	@echo ""
	@echo "Main Commands:"
	@echo "  make -f local-network.mk setup     - Create directories and generate keys"
	@echo "  make -f local-network.mk start     - Start network (gateways and nodes)"
	@echo "  make -f local-network.mk stop      - Stop all processes"
	@echo "  make -f local-network.mk clean     - Clean all files and stop processes"
	@echo "  make -f local-network.mk status    - Show network status"
	@echo ""
	@echo "Node Management:"
	@echo "  make -f local-network.mk logs node=n1     - Show logs for node 1"
	@echo "  make -f local-network.mk logs node=gw1    - Show logs for gateway 1"
	@echo "  make -f local-network.mk stop-node node=n1- Stop node 1"
	@echo ""
	@echo "Configuration:"
	@echo "  N_NODES=$(N_NODES), N_GATEWAYS=$(N_GATEWAYS)"
	@echo "  Base ports: network=$(BASE_PORT), websocket=$(WS_BASE_PORT)"

# Setup Commands
# ------------------------------------------
setup: build-freenet create-dirs generate-keys

build-freenet:
	@echo "→ Building Freenet..."
	@cargo install --path $(FREENET_CORE_PATH) --features "local-simulation"

create-dirs:
	@echo "→ Creating directories..."
	@mkdir -p $(KEYS_DIR) $(PID_DIR) $(LOGS_DIR)
	@for i in $$(seq 1 $(N_GATEWAYS)); do mkdir -p $(BASE_DIR)/gw$$i; done
	@for i in $$(seq 1 $(N_NODES)); do mkdir -p $(BASE_DIR)/n$$i; done

generate-keys:
	@echo "→ Generating RSA keys..."
	@for i in $$(seq 1 $(N_GATEWAYS)); do \
		if [ ! -f "$(KEYS_DIR)/gw$${i}_private_key.pem" ]; then \
			openssl genpkey -algorithm RSA -out $(KEYS_DIR)/gw$${i}_private_key.pem -pkeyopt rsa_keygen_bits:4096 && \
			openssl pkey -in $(KEYS_DIR)/gw$${i}_private_key.pem \
				-pubout -out $(KEYS_DIR)/gw$${i}_public_key.pem; \
			echo "  Generated keys for gateway $$i"; \
		else \
			echo "  Keys for gateway $$i already exist"; \
		fi; \
	done

# Network Management
# ------------------------------------------
start: stop start-gateways start-nodes
	@echo "→ Network started successfully"
	@$(MAKE) -f local-network.mk status

start-gateways: generate-gw-config
	@echo "→ Starting gateways..."
	@for i in $$(seq 1 $(N_GATEWAYS)); do \
		port=$$(( $(BASE_PORT) + $$i )); \
		ws_port=$$(( $(WS_BASE_PORT) + $$i )); \
		if [ -f "$(LOGS_DIR)/gw$$i.log" ]; then : > "$(LOGS_DIR)/gw$$i.log"; fi; \
		($(ENV_VARS) freenet network \
			--is-gateway \
			--ws-api-port $$ws_port \
			--public-network-address 127.0.0.1 \
			--public-network-port $$port \
			--db-dir $(BASE_DIR)/gw$$i \
			--transport-keypair $(KEYS_DIR)/gw$${i}_private_key.pem \
			--network-port $$port $(call LOG_CMD,$(LOGS_DIR)/gw$$i.log)) & \
		echo $$! > $(PID_DIR)/gw$$i.pid; \
		echo "  Gateway $$i: port=$$port, ws=$$ws_port (PID: $$!)"; \
	done
	@echo "  Waiting 2 seconds for gateways to initialize..."
	@sleep 2

start-nodes:
	@echo "→ Starting nodes..."
	@for i in $$(seq 1 $(N_NODES)); do \
		network_port=$$(( $(BASE_PORT) + $(N_GATEWAYS) + $$i )); \
		ws_port=$$(( $(WS_BASE_PORT) + $(N_GATEWAYS) + $$i )); \
		public_port=$$(( $(WS_BASE_PORT) + $(N_NODES) + $$i )); \
		if [ -f "$(LOGS_DIR)/n$$i.log" ]; then : > $(LOGS_DIR)/n$$i.log; fi; \
		($(ENV_VARS) freenet network \
			--config-dir $(BASE_DIR) \
			--ws-api-port $$ws_port \
			--public-network-port $$public_port \
			--db-dir $(BASE_DIR)/n$$i \
			--network-port $$network_port \
			$(call LOG_CMD,$(LOGS_DIR)/n$$i.log)) & \
		echo $$! > $(PID_DIR)/n$$i.pid; \
		echo "  Node $$i: network=$$network_port, ws=$$ws_port, public=$$public_port (PID: $$!)"; \
		if [ $$i -lt $(N_NODES) ]; then \
			echo "  Waiting 1 second before starting next node..."; \
			sleep 1; \
		fi; \
	done

generate-gw-config:
	@echo "→ Generating gateway configuration..."
	@echo "# Freenet Gateway Configuration" > $(GW_CONFIG)
	@for i in $$(seq 1 $(N_GATEWAYS)); do \
		port=$$(( $(BASE_PORT) + $$i )); \
		echo "" >> $(GW_CONFIG); \
		echo "[[gateways]]" >> $(GW_CONFIG); \
		echo "address = { host_address = \"127.0.0.1:$$port\" }" >> $(GW_CONFIG); \
		echo "public_key = \"$(KEYS_DIR)/gw$${i}_public_key.pem\"" >> $(GW_CONFIG); \
	done

# Monitoring and Control
# ------------------------------------------
logs:
	@if [ -z "$(node)" ]; then \
		echo "Usage: make -f local-network.mk logs node=<node_name> (e.g., n1, gw1)"; \
		exit 1; \
	fi
	@if [ ! -f "$(LOGS_DIR)/$(node).log" ]; then \
		echo "No logs found for $(node)"; \
		exit 1; \
	fi
	@tail -f $(LOGS_DIR)/$(node).log

status:
	@echo "Network Status:"
	@echo "→ Gateways:"
	@for i in $$(seq 1 $(N_GATEWAYS)); do \
		if [ -f "$(PID_DIR)/gw$$i.pid" ]; then \
			pid=$$(cat "$(PID_DIR)/gw$$i.pid"); \
			if kill -0 $$pid 2>/dev/null; then \
				echo "  Gateway $$i: Running (PID: $$pid)"; \
			else \
				echo "  Gateway $$i: Crashed (PID: $$pid)"; \
			fi; \
		else \
			echo "  Gateway $$i: Stopped"; \
		fi; \
	done
	@echo "→ Nodes:"
	@for i in $$(seq 1 $(N_NODES)); do \
		if [ -f "$(PID_DIR)/n$$i.pid" ]; then \
			pid=$$(cat "$(PID_DIR)/n$$i.pid"); \
			if kill -0 $$pid 2>/dev/null; then \
				echo "  Node $$i: Running (PID: $$pid)"; \
			else \
				echo "  Node $$i: Crashed (PID: $$pid)"; \
			fi; \
		else \
			echo "  Node $$i: Stopped"; \
		fi; \
	done

# Cleanup
# ------------------------------------------
stop-node:
	@if [ -z "$(node)" ]; then \
		echo "Usage: make -f local-network.mk stop-node node=<node_name> (e.g., n1, gw1)"; \
		exit 1; \
	fi
	@if [ -f "$(PID_DIR)/$(node).pid" ]; then \
		pid=$$(cat "$(PID_DIR)/$(node).pid"); \
		kill $$pid 2>/dev/null || true; \
		rm "$(PID_DIR)/$(node).pid"; \
		echo "→ $(node) stopped (PID: $$pid)"; \
	else \
		echo "→ $(node) is not running"; \
	fi

stop:
	@echo "→ Stopping all processes..."
	@# Stop processes using PID files
	@for pid_file in $(PID_DIR)/*.pid; do \
		if [ -f "$$pid_file" ]; then \
			pid=$$(cat "$$pid_file"); \
			if kill -0 $$pid 2>/dev/null; then \
				kill $$pid 2>/dev/null || true; \
				echo "  Stopped process PID: $$pid"; \
			else \
				echo "  Process PID $$pid not running"; \
			fi; \
			rm -f "$$pid_file"; \
		fi; \
	done; \
	# Then find and stop any remaining freenet processes, excluding make and the current shell
	@current_pid=$$$$; \
	make_pid=$$(ps -o ppid= -p $$current_pid | tr -d ' '); \
	pids=$$(pgrep -f "freenet network" | grep -v $$current_pid | grep -v $$make_pid || true); \
	if [ ! -z "$$pids" ]; then \
		echo "  Found additional freenet processes..."; \
		for pid in $$pids; do \
			if kill -0 $$pid 2>/dev/null; then \
				kill $$pid 2>/dev/null || true; \
				echo "  Stopped additional process (PID: $$pid)"; \
			fi; \
		done; \
	fi
	@echo "→ All processes stopped"

clean: stop
	@echo "→ Cleaning all files..."
	@rm -rf $(BASE_DIR)
	@echo "→ Cleanup complete"
