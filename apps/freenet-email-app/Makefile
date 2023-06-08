# expected make version >= 3.82

.ONESHELL:

LOCUTUS_DIR := $(abspath ../../)
INBOX_WEB_DIR := $(LOCUTUS_DIR)/apps/freenet-email-app/web
INBOX_DIR := $(LOCUTUS_DIR)/apps/freenet-email-app/contracts/inbox
AFT_RECORD := $(LOCUTUS_DIR)/modules/antiflood-tokens/contracts/token-allocation-record
AFT_GENERATOR := $(LOCUTUS_DIR)/modules/antiflood-tokens/delegates/token-generator
WEB_DIR_SRC := $(LOCUTUS_DIR)/apps/freenet-email-app/web/src

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

.PHONY: all build-inbox build-token-allocation clean

all:
	$(MAKE) build-tool &
	$(MAKE) build-inbox &
	$(MAKE) build-token-allocation &
	$(MAKE) run-node &
	$(MAKE) run-web &
	wait

build: \
	build-tool \
	build-inbox \
	build-token-allocation

build-inbox: \
	build-inbox-0 publish-inbox-0 \
	build-inbox-1 publish-inbox-1 \
	generate-inbox-contract-code-hash

build-token-allocation: \
	build-token-allocation-contract-0 publish-token-allocation-contract-0 \
	build-token-allocation-contract-1 publish-token-allocation-contract-1 \
	build-token-generator-0 publish-token-generator-delegate-0 \
	build-token-generator-1 publish-token-generator-delegate-1 

build-tool:
	cd $(LOCUTUS_DIR)/crates/locutus-node && cargo build --release
	cargo install --path $(LOCUTUS_DIR)/crates/locutus-node

build-inbox-%:
	cd $(INBOX_DIR) && cargo run --bin gen_initial_state -- $* "$(WEB_DIR_SRC)/../examples/rsa4096-id-$*-priv.pem"
	cd $(INBOX_DIR) && cargo run --bin gen_pub_key -- $* "$(WEB_DIR_SRC)/../examples/rsa4096-id-$*-priv.pem"
	cd $(INBOX_DIR) && ldt build --features contract

publish-inbox-%:
	cd $(INBOX_DIR) && ldt publish --code build/locutus/freenet_email_inbox --parameters examples/inbox_key_$* contract --state build/locutus/contract-state

build-token-allocation-contract-%:
	cd $(AFT_RECORD)
	cargo run --bin gen_pub_key -- $* "$(WEB_DIR_SRC)/../examples/rsa4096-id-$*-priv.pem"
	cargo run --bin gen_initial_state -- $* "$(WEB_DIR_SRC)/../examples/rsa4096-id-$*-priv.pem" "$(WEB_DIR_SRC)/../examples/rsa4096-id-$*-priv.pem"; \
	ldt build

publish-token-allocation-contract-%:
	cd $(AFT_RECORD) && ldt publish --code build/locutus/locutus_token_allocation_record --parameters examples/generator_pub_key_$* contract --state build/locutus/contract-state

build-token-generator-%:
	cd $(AFT_GENERATOR)
	cargo run --bin gen_pub_key -- $* "$(WEB_DIR_SRC)/../examples/rsa4096-id-$*-priv.pem"
	ldt build --package-type delegate --features node

publish-token-generator-delegate-%:
	cd $(AFT_GENERATOR) && ldt publish --code build/locutus/locutus_token_generator --parameters examples/generator_pub_key_$* delegate

generate-inbox-contract-code-hash:
	cd $(INBOX_DIR)
	hash=$$(bash -c "ldt inspect build/locutus/freenet_email_inbox code | grep 'code hash:' | cut -d' ' -f3")
	mkdir -p $(WEB_DIR_SRC)/../build
	echo -n $$hash > $(WEB_DIR_SRC)/../build/inbox_code_hash

run-node:
	RUST_BACKTRACE=1 RUST_LOG=locutus=debug,locutus_core=debug,info locutus-node local

run-web:
	cd $(INBOX_WEB_DIR)
	trunk build --features use-node --no-default-features
	trunk serve --features use-node --no-default-features --port 57616

clean:
	rm -rf $(CARGO_TARGET_DIR)
