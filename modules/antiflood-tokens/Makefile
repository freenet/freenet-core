# expected make version >= 3.82

.ONESHELL:

LOCUTUS_DIR := $(abspath ../../)
AFT_RECORD := $(LOCUTUS_DIR)/modules/antiflood-tokens/contracts/token-allocation-record
AFT_GENERATOR := $(LOCUTUS_DIR)/modules/antiflood-tokens/delegates/token-generator

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

.PHONY: all build-token-allocation clean

all:
	$(MAKE) build-tool &
	$(MAKE) build-token-allocation &

build-token-allocation: \
	build-token-allocation-contract \
	build-token-generator \
	generate-token-allocation-code-hash generate-token-generator-delegate-code-hash

build-tool:
	cd $(LOCUTUS_DIR)/crates/locutus-node && cargo build --release
	cargo install --path $(LOCUTUS_DIR)/crates/locutus-node
	
build-token-allocation-contract:
	cd $(AFT_RECORD)
	ldt build

build-token-generator:
	cd $(AFT_GENERATOR)
	ldt build --package-type delegate

generate-token-allocation-code-hash:
	cd $(AFT_RECORD)
	hash=$$(bash -c "ldt inspect build/locutus/locutus_token_allocation_record code | grep 'code hash:' | cut -d' ' -f3")
	mkdir -p $(AFT_RECORD)/build
	echo -n $$hash > $(AFT_RECORD)/build/token_allocation_record_code_hash

generate-token-generator-delegate-code-hash:
	cd $(AFT_GENERATOR)
	hash=$$(bash -c "ldt inspect build/locutus/locutus_token_generator delegate | grep 'code hash:' | cut -d' ' -f3")
	mkdir -p $(AFT_GENERATOR)/build
	echo -n $$hash > $(AFT_GENERATOR)/build/token_generator_code_hash

clean:
	rm -rf $(CARGO_TARGET_DIR)
