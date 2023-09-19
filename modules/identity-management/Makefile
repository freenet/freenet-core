# expected make version >= 3.82

.ONESHELL:

LOCUTUS_DIR := $(abspath ../../)
DELEGATE_DIR := $(LOCUTUS_DIR)/modules/identity-management

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

.PHONY: all clean

all:
	$(MAKE) build-tool &&
	$(MAKE) build

build-tool:
	cd $(LOCUTUS_DIR)/crates/freenet-node && cargo build --release
	cargo install --path $(LOCUTUS_DIR)/crates/freenet-node
	
build: \
	build-delegate \
	generate-delegate-code-hash \
	generate-delegate-params

build-delegate:
	cd $(DELEGATE_DIR)
	fdev build --package-type delegate --features contract

generate-delegate-code-hash:
	cd $(DELEGATE_DIR)
	hash=$$(bash -c "fdev inspect build/freenet/identity_management delegate | grep 'code hash:' | cut -d' ' -f3")
	mkdir -p $(DELEGATE_DIR)/build
	echo -n $$hash > $(DELEGATE_DIR)/build/identity_management_code_hash

generate-delegate-params:
	cd $(DELEGATE_DIR)
	cargo run -- --path $(DELEGATE_DIR)/build

clean:
	rm -rf $(CARGO_TARGET_DIR)
