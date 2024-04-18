# expected make version >= 3.82

#.ONESHELL:

ifeq ($(OS), Windows_NT)
	ROOT := $(abspath ./)
	PATH_SEP := \\
else
	ROOT := $(abspath ./)
	PATH_SEP := /
endif

FREENET_ROOT := $(ROOT)$(PATH_SEP)..$(PATH_SEP)..
INBOX_WEB_DIR := $(ROOT)$(PATH_SEP)web
INBOX_DIR := $(ROOT)$(PATH_SEP)contracts$(PATH_SEP)inbox
AFT_RECORD := $(FREENET_ROOT)$(PATH_SEP)modules$(PATH_SEP)antiflood-tokens$(PATH_SEP)contracts$(PATH_SEP)token-allocation-record
AFT_GENERATOR := $(FREENET_ROOT)$(PATH_SEP)modules$(PATH_SEP)antiflood-tokens$(PATH_SEP)delegates$(PATH_SEP)token-generator
IDENTITY_DELEGATE := $(FREENET_ROOT)$(PATH_SEP)modules$(PATH_SEP)identity-management
WEB_DIR_SRC := $(ROOT)$(PATH_SEP)web$(PATH_SEP)src
FILE := $(IDENTITY_DELEGATE)$(PATH_SEP)build$(PATH_SEP)identity-manager-key.private.pem

ifeq ($(CARGO_TARGET_DIR),)
	$(error CARGO_TARGET_DIR is not set)
endif

all: install-deps build

install-deps:
	cargo install dioxus-cli

build: \
	generate-id-manager-key \
	build-inbox \
	build-webapp \
	publish-webapp

generate-id-manager-key:
	@if [ ! -f "$(FILE)" ]; then \
		cd "$(IDENTITY_DELEGATE)" && cargo run -- --path "$(IDENTITY_DELEGATE)$(PATH_SEP)build"; \
	fi

build-inbox:
	cd $(INBOX_DIR) && fdev build --features contract && \
	hash=$$(bash -c "fdev inspect build$(PATH_SEP)freenet$(PATH_SEP)freenet_email_inbox code | grep 'code hash:' | cut -d' ' -f3") && \
	mkdir -p $(WEB_DIR_SRC)$(PATH_SEP)..$(PATH_SEP)build && \
	echo -n $$hash > $(WEB_DIR_SRC)$(PATH_SEP)..$(PATH_SEP)build$(PATH_SEP)inbox_code_hash

run-node:
	RUST_BACKTRACE=1 RUST_LOG=freenet=debug,freenet_core=debug,info freenet local

run-web:
	cd $(INBOX_WEB_DIR) && dx build --target index.html --platform web --features use-node && \
	dx serve --features use-node --port 57616

build-webapp:
	cd $(INBOX_WEB_DIR) && dx build --target index.html --release --platform web --features use-node && \
	sed -i 's@/./assets@./assets@g' dist/index.html && \
	fdev build

publish-webapp:
	cd $(INBOX_WEB_DIR) && fdev publish --code build$(PATH_SEP)freenet$(PATH_SEP)freenet_email_app_web contract --state build$(PATH_SEP)freenet$(PATH_SEP)contract-state

clean:
	rm -rf $(CARGO_TARGET_DIR)
