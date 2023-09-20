# expected make version >= 3.82

.ONESHELL:

FREENET_DIR := $(abspath ../../)
AFT_RECORD := $(FREENET_DIR)/modules/antiflood-tokens/contracts/token-allocation-record
AFT_GENERATOR := $(FREENET_DIR)/modules/antiflood-tokens/delegates/token-generator

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

.PHONY: all clean

all:
	$(MAKE) build-tool &&
	$(MAKE) build-token-allocation

build: \
	build-token-allocation-contract \
	build-token-generator \
	generate-token-allocation-code-hash generate-token-generator-delegate-code-hash

build-tool:
	cargo install --force --path $(FREENET_DIR)/crates/core
	cargo install --force --path $(FREENET_DIR)/crates/fdev
	
build-token-allocation-contract:
	cd $(AFT_RECORD)
	fdev build

build-token-generator:
	cd $(AFT_GENERATOR)
	fdev build --package-type delegate

generate-token-allocation-code-hash:
	cd $(AFT_RECORD)
	hash=$$(bash -c "fdev inspect build/freenet/freenet_token_allocation_record code | grep 'code hash:' | cut -d' ' -f3")
	mkdir -p $(AFT_RECORD)/build
	echo -n $$hash > $(AFT_RECORD)/build/token_allocation_record_code_hash

generate-token-generator-delegate-code-hash:
	cd $(AFT_GENERATOR)
	hash=$$(bash -c "fdev inspect build/freenet/freenet_token_generator delegate | grep 'code hash:' | cut -d' ' -f3")
	mkdir -p $(AFT_GENERATOR)/build
	echo -n $$hash > $(AFT_GENERATOR)/build/token_generator_code_hash

clean:
	rm -rf $(CARGO_TARGET_DIR)
