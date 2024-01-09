# expected make version >= 3.82

#.ONESHELL:

ifeq ($(OS), Windows_NT)
	ROOT := $(abspath .\)
	FREENET_ROOT := $(ROOT)\..\..\..\freenet-core
	INBOX_WEB_DIR := $(ROOT)\web
	INBOX_DIR := $(ROOT)\contracts\inbox
	AFT_RECORD := $(FREENET_ROOT)\modules\antiflood-tokens\contracts\token-allocation-record
	AFT_GENERATOR := $(FREENET_ROOT)\modules\antiflood-tokens\delegates\token-generator
	IDENTITY_DELEGATE := $(FREENET_ROOT)\modules\identity-management
	WEB_DIR_SRC := $(ROOT)\web\src
	FILE := $(IDENTITY_DELEGATE)\build\identity-manager-key.private.pem
else
	ROOT := $(abspath ./)
	INBOX_WEB_DIR := $(ROOT)/web
	INBOX_DIR := $(ROOT)/contracts/inbox
	AFT_RECORD := $(FREENET_ROOT)/modules/antiflood-tokens/contracts/token-allocation-record
	AFT_GENERATOR := $(FREENET_ROOT)/modules/antiflood-tokens/delegates/token-generator
	IDENTITY_DELEGATE := $(FREENET_ROOT)/modules/identity-management
	WEB_DIR_SRC := $(ROOT)/web/src
endif


ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

#.PHONY: build clean


ifeq ($(OS), Windows_NT)
all:
	echo "root: $(ROOT)"
	echo "freenet root: $(FREENET_ROOT)"
	echo "cargo target dir: $(CARGO_TARGET_DIR)"
	$(MAKE) install-deps
	$(MAKE) build

install-deps:
	cargo install dioxus-cli

build: \
	build-aft-record \
	generate-id-manager-key \
	build-inbox \
	build-webapp \
	publish-webapp

build-aft-record:
	echo $(AFT_RECORD)
	cd $(AFT_RECORD) && fdev build

generate-id-manager-key:
	echo $(FILE)
ifeq ("$(wildcard $(FILE))","")
	cd $(IDENTITY_DELEGATE)
	cargo run -- --path "$(IDENTITY_DELEGATE)\build"
endif
	
	setx FILE $(IDENTITY_DELEGATE)\build\identity-manager-key.private.pem
	echo "file: $(FILE)"

build-inbox:
	cd $(INBOX_DIR) && fdev build --features contract
	fdev inspect $(INBOX_DIR)\build\freenet\freenet_email_inbox code
	$(eval HASH_LINE=$(shell fdev inspect $(INBOX_DIR)\build\freenet\freenet_email_inbox code))
	echo $(subst "code hash: , ,$(HASH_LINE))
	$(eval HASH_LINE=$(subst code hash: ,,$(HASH_LINE)))
	$(eval HASH=$(subst contract API version: 0.0.1,,$(HASH_LINE)))
	echo $(HASH)
	$(shell mkdir "$(WEB_DIR_SRC)\..\build")
	echo $(HASH) > $(WEB_DIR_SRC)\..\build\inbox_code_hash

run-node:
	setx RUST_BACKTRACE 1 
	setx RUST_LOG debug
	setx freenet debug
	setx freenet_core debug,info
	freenet local

run-web:
	cd $(INBOX_WEB_DIR) && dx build --target index.html --platform web --features use-node
	cd $(INBOX_WEB_DIR) && dx serve --features use-node --port 57616

build-webapp:
	cd $(INBOX_WEB_DIR) && dx build --target index.html --release --platform web --features use-node
	cd $(INBOX_WEB_DIR) && sed -i "s@/./assets@./assets@g" dist/index.html
	cd $(INBOX_WEB_DIR) && fdev build

publish-webapp:
	cd $(INBOX_WEB_DIR) && fdev publish --code build\freenet\freenet_email_app_web contract --state build\freenet\contract-state

clean:
	rm -rf $(CARGO_TARGET_DIR)
else
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
	if [ ! -f $$FILE ]; then
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
endif


