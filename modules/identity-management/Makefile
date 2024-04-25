# expected make version >= 3.82

.ONESHELL:

FREENET_DIR := $(abspath ../../)
DELEGATE_DIR := $(FREENET_DIR)/modules/identity-management

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

.PHONY: all clean

all:
	$(MAKE) build-tool &&
	$(MAKE) build

build-tool:
	cargo install --path $(FREENET_DIR)/crates/core
	cargo install --path $(FREENET_DIR)/crates/fdev
	
build: \
	build-delegate \
	generate-delegate-code-hash \
	generate-delegate-params

build-delegate:
	cd $(DELEGATE_DIR)
	fdev build --package-type delegate --features contract

ifeq ($(OS), Windows_NT)
generate-delegate-code-hash:
	echo "TMP"
	cd $(DELEGATE_DIR)
	fdev inspect "$(DELEGATE_DIR)/build/freenet/identity_management" delegate
	$(eval HASH_LINE=$(shell fdev inspect "$(DELEGATE_DIR)/build/freenet/identity_management" delegate))
	echo $(HASH_LINE)
	echo $(subst "code hash: ,,$(HASH_LINE))
	$(eval HASH_LINE=$(subst code hash: ,,$(HASH_LINE)))
	$(eval HASH=$(subst contract API version: 0.0.1,,$(HASH_LINE)))
	mkdir "$(DELEGATE_DIR)/build"
	echo "hash: $(HASH)"
	echo $(HASH) > $(DELEGATE_DIR)/build/identity_management_code_hash
else
generate-delegate-code-hash:
	cd $(DELEGATE_DIR)
	hash=$$(bash -c "fdev inspect build/freenet/identity_management delegate | grep 'code hash:' | cut -d' ' -f3")
	mkdir -p $(DELEGATE_DIR)/build
	echo -n $$hash > $(DELEGATE_DIR)/build/identity_management_code_hash
endif


generate-delegate-params:
	cd $(DELEGATE_DIR)
	cargo run -- --path $(DELEGATE_DIR)/build

clean:
	rm -rf $(CARGO_TARGET_DIR)
