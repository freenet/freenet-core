# expected make version >= 3.82

.ONESHELL:

LOCUTUS_DIR := $(shell cd ../../ && pwd)
INBOX_WEB_DIR := $(LOCUTUS_DIR)/apps/freenet-email-app/web
INBOX_DIR := $(LOCUTUS_DIR)/apps/freenet-email-app/contracts/inbox
INBOX_SRC_DIR := $(INBOX_DIR)/src
export CARGO_TARGET_DIR := $(LOCUTUS_DIR)/target

.PHONY: all build clean

all: build run-node run-web

build: build-tool build-and-publish-1 build-and-publish-2

build-tool:
	cd $(LOCUTUS_DIR)/crates/locutus-node && cargo build --bin ldt

build-and-publish-1: build-inbox-1 publish-inbox-1
build-and-publish-2: build-inbox-2 publish-inbox-2

build-inbox-%:
	cd $(INBOX_SRC_DIR) && cargo run --bin gen_initial_state -- $* "../../../web/examples/rsa4096-id-$*-priv.pem"
	cd $(INBOX_SRC_DIR) && cargo run --bin gen_pub_key -- $* "../../../web/examples/rsa4096-id-$*-priv.pem"
	cd $(INBOX_DIR) && ${CARGO_TARGET_DIR}/debug/ldt build --features contract

publish-inbox-%:
	cd $(INBOX_DIR) && ${CARGO_TARGET_DIR}/debug/ldt publish --code build/locutus/freenet_email_inbox --state build/locutus/contract-state --parameters examples/inbox_key_$*

run-node:
	 ${CARGO_TARGET_DIR}/debug/locutus-node local

run-web:
	cd $(INBOX_WEB_DIR)
	cargo build --features use-nose
	trunk serve

clean:
	rm -rf $(CARGO_TARGET_DIR)
