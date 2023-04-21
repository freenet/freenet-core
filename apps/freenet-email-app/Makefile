# expected make version >= 3.82

.ONESHELL:

LOCUTUS_DIR := $(abspath ../../)
INBOX_WEB_DIR := $(LOCUTUS_DIR)/apps/freenet-email-app/web
INBOX_DIR := $(LOCUTUS_DIR)/apps/freenet-email-app/contracts/inbox
INBOX_SRC_DIR := $(INBOX_DIR)/src
WEB_DIR_SRC := $(LOCUTUS_DIR)/apps/freenet-email-app/web/src

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

.PHONY: all build clean

all:
	$(MAKE) build
	$(MAKE) run-node &
	$(MAKE) run-web &
	wait

build: build-tool build-inbox-0 publish-inbox-0 build-inbox-1 publish-inbox-1 generate-contract-code-hash

build-tool:
	cd $(LOCUTUS_DIR)/crates/locutus-node && cargo build --release
	cargo install --path $(LOCUTUS_DIR)/crates/locutus-node

build-inbox-%:
	cd $(INBOX_SRC_DIR) && cargo run --bin gen_initial_state -- $* "../../../web/examples/rsa4096-id-$*-priv.pem"
	cd $(INBOX_SRC_DIR) && cargo run --bin gen_pub_key -- $* "../../../web/examples/rsa4096-id-$*-priv.pem"
	cd $(INBOX_DIR) && ldt build --features contract

publish-inbox-%:
	cd $(INBOX_DIR) && ldt publish --code build/locutus/freenet_email_inbox --state build/locutus/contract-state --parameters examples/inbox_key_$*

generate-contract-code-hash:
	cd $(WEB_DIR_SRC) && cargo run --bin get_code_hash --features use-node,build-binary

run-node:
	RUST_LOG=debug locutus-node local

run-web:
	cd $(INBOX_WEB_DIR)
	trunk build --features use-node --no-default-features
	trunk serve --features use-node --no-default-features

clean:
	rm -rf $(CARGO_TARGET_DIR)
