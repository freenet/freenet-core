# expected make version >= 3.82

.ONESHELL:

FREENET_DIR := $(abspath ../../)
INBOX_WEB_DIR := $(FREENET_DIR)/apps/freenet-email-app/web
INBOX_DIR := $(FREENET_DIR)/apps/freenet-email-app/contracts/inbox
AFT_RECORD := $(FREENET_DIR)/modules/antiflood-tokens/contracts/token-allocation-record
AFT_GENERATOR := $(FREENET_DIR)/modules/antiflood-tokens/delegates/token-generator
IDENTITY_DELEGATE := $(FREENET_DIR)/modules/identity-management
WEB_DIR_SRC := $(FREENET_DIR)/apps/freenet-email-app/web/src

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

.PHONY: build clean

all: \
	$(MAKE) build-tool \
	$(MAKE) build

build: \
	generate-id-manager-key \
	build-inbox \
	build-webapp \
	publish-webapp

build-tool:
	cargo install --force --path $(FREENET_DIR)/crates/core
	cargo install --force --path $(FREENET_DIR)/crates/fdev

generate-id-manager-key:
	FILE="$(IDENTITY_DELEGATE)/build/identity-manager-key.private.pem"
	if [ ! -f $$FILE ] 
	then 
		cd $(IDENTITY_DELEGATE) && cargo run -- --path "$(IDENTITY_DELEGATE)/build/"
	fi

build-inbox:
	cd $(INBOX_DIR)
	fdev build --features contract
	hash=$$(bash -c "fdev inspect build/freenet/freenet_email_inbox code | grep 'code hash:' | cut -d' ' -f3")
	mkdir -p $(WEB_DIR_SRC)/../build
	echo -n $$hash > $(WEB_DIR_SRC)/../build/inbox_code_hash

run-node:
	RUST_BACKTRACE=1 RUST_LOG=freenet=debug,freenet_core=debug,info freenet local

run-web:
	cd $(INBOX_WEB_DIR)
	dx build --target index.html --platform web --features use-node
	dx serve --features use-node --port 57616

build-webapp:
	cd $(INBOX_WEB_DIR)
	dx build --target index.html --release --platform web --features use-node
	sed -i 's@/./assets@./assets@g' dist/index.html
	fdev build

publish-webapp:
	cd $(INBOX_WEB_DIR)
	fdev publish --code build/freenet/freenet_email_app_web contract --state build/freenet/contract-state

clean:
	rm -rf $(CARGO_TARGET_DIR)
