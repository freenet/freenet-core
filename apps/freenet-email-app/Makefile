# expected make version >= 3.82

.ONESHELL:

LOCUTUS_DIR := $(abspath ../../)
INBOX_WEB_DIR := $(LOCUTUS_DIR)/apps/freenet-email-app/web
INBOX_DIR := $(LOCUTUS_DIR)/apps/freenet-email-app/contracts/inbox
AFT_RECORD := $(LOCUTUS_DIR)/modules/antiflood-tokens/contracts/token-allocation-record
AFT_GENERATOR := $(LOCUTUS_DIR)/modules/antiflood-tokens/delegates/token-generator
IDENTITY_DELEGATE := $(LOCUTUS_DIR)/modules/identity-management
WEB_DIR_SRC := $(LOCUTUS_DIR)/apps/freenet-email-app/web/src

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

.PHONY: build clean

build: \
	build-tool \
	generate-id-manager-key \
	build-inbox 

build-tool:
	cd $(LOCUTUS_DIR)/crates/locutus-node && cargo build --release
	cargo install --path $(LOCUTUS_DIR)/crates/locutus-node

generate-id-manager-key:
	FILE="$(IDENTITY_DELEGATE)/build/identity-manager-key.private.pem"
	if [ ! -f $$FILE ]; then
		cd $(IDENTITY_DELEGATE) && cargo run -- --path "$(IDENTITY_DELEGATE)/build/"
	fi

build-inbox: \
	generate-inbox-contract-code-hash

generate-inbox-contract-code-hash:
	cd $(INBOX_DIR)
	ldt build --features contract
	hash=$$(bash -c "ldt inspect build/locutus/freenet_email_inbox code | grep 'code hash:' | cut -d' ' -f3")
	mkdir -p $(WEB_DIR_SRC)/../build
	echo -n $$hash > $(WEB_DIR_SRC)/../build/inbox_code_hash

run-node:
	RUST_BACKTRACE=1 RUST_LOG=locutus=debug,locutus_core=debug,info locutus-node local

run-web:
	cd $(INBOX_WEB_DIR)
	trunk build --features use-node --no-default-features
	trunk serve --features use-node --no-default-features --port 57616

build-webapp:
	cd $(INBOX_WEB_DIR)
	ldt build

publish-webapp:
	cd $(INBOX_WEB_DIR)
	ldt publish --code build/locutus/freenet_email_app_web contract --state build/locutus/contract-state

clean:
	rm -rf $(CARGO_TARGET_DIR)
