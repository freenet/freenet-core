# expected make version >= 3.82

.ONESHELL:

LOCUTUS_DIR := $(abspath ../../)
INBOX_WEB_DIR := $(LOCUTUS_DIR)/apps/freenet-email-app/web
INBOX_DIR := $(LOCUTUS_DIR)/apps/freenet-email-app/contracts/inbox
INBOX_SRC_DIR := $(INBOX_DIR)/src
TOKEN_ALLOCATION_DIR := $(LOCUTUS_DIR)/modules/antiflood-tokens/contracts/token-allocation-record
WEB_DIR_SRC := $(LOCUTUS_DIR)/apps/freenet-email-app/web/src

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

.PHONY: all build-inbox build-token-allocation clean

all:
	$(MAKE) build-inbox &
	$(MAKE) build-token-allocation &
	$(MAKE) run-node &
	$(MAKE) run-web &
	wait

build-inbox: build-tool build-inbox-0 publish-inbox-0 build-inbox-1 publish-inbox-1 generate-inbox-contract-code-hash
build-token-allocation: build-token-allocation-contract-0 publish-token-allocation-contract-0 build-token-allocation-contract-1 publish-token-allocation-contract-1 generate-token-allocation-code-hash

build-tool:
	cd $(LOCUTUS_DIR)/crates/locutus-node && cargo build --release
	cargo install --path $(LOCUTUS_DIR)/crates/locutus-node

build-inbox-%:
	cd $(INBOX_SRC_DIR) && cargo run --bin gen_initial_state -- $* "$(WEB_DIR_SRC)/../examples/rsa4096-id-$*-priv.pem"
	cd $(INBOX_SRC_DIR) && cargo run --bin gen_pub_key -- $* "$(WEB_DIR_SRC)/../examples/rsa4096-id-$*-priv.pem"
	cd $(INBOX_DIR) && ldt build --features contract

publish-inbox-%:
	cd $(INBOX_DIR) && ldt publish --code build/locutus/freenet_email_inbox --state build/locutus/contract-state --parameters examples/inbox_key_$*

build-token-allocation-contract-%:
	cd $(TOKEN_ALLOCATION_DIR)
	cargo run --bin gen_pub_key -- $* "$(WEB_DIR_SRC)/../examples/rsa4096-id-$*-priv.pem"
	if [ $* -eq 0 ]; then \
		cargo run --bin gen_initial_state -- $* "$(WEB_DIR_SRC)/../examples/rsa4096-id-0-priv.pem" "$(WEB_DIR_SRC)/../examples/rsa4096-id-1-priv.pem"; \
	else \
		cargo run --bin gen_initial_state -- $* "$(WEB_DIR_SRC)/../examples/rsa4096-id-1-priv.pem" "$(WEB_DIR_SRC)/../examples/rsa4096-id-0-priv.pem"; \
	fi
	ldt build

publish-token-allocation-contract-%:
	cd $(TOKEN_ALLOCATION_DIR) && ldt publish --code build/locutus/locutus_token_allocation_record --state build/locutus/contract-state --parameters examples/generator_pub_key_$*

generate-inbox-contract-code-hash:
	cd $(INBOX_DIR)
	hash=$$(bash -c "ldt inspect build/locutus/freenet_email_inbox code | grep 'code hash:' | cut -d' ' -f3")
	echo $$hash > $(WEB_DIR_SRC)/../dist/inbox_code_hash

generate-token-allocation-code-hash:
	cd $(TOKEN_ALLOCATION_DIR)
	hash=$$(bash -c "ldt inspect build/locutus/locutus_token_allocation_record code | grep 'code hash:' | cut -d' ' -f3")
	echo $$hash > $(WEB_DIR_SRC)/../dist/token_allocation_record_code_hash

run-node:
	RUST_LOG=debug locutus-node local

run-web:
	cd $(INBOX_WEB_DIR)
	trunk build --features use-node --no-default-features
	trunk serve --features use-node --no-default-features

clean:
	rm -rf $(CARGO_TARGET_DIR)
