# expected make version >= 3.82

.ONESHELL:

VIEW_DIR =./view
VIEW_FILES=$(wildcard ./view/*)

CONTROLLER_DIR =./controller
CONTROLLER_FILES=$(wildcard ./controllers/*)

LOCUTUS_DIR=$(shell cd ../.. && pwd)

view: $(VIEW_FILES)
	cd $(LOCUTUS_DIR)
	cd apps/freenet-microblogging
	cd $(VIEW_DIR)

	CARGO_TARGET_DIR=$(LOCUTUS_DIR)/target/
	bash compile_contract.sh
	mv $(LOCUTUS_DIR)/target/freenet_microblogging_view.wasm ../../../crates/http-gw/examples/

	cd $(LOCUTUS_DIR)
	cd crates/locutus-dev
	cargo run --bin locutus-dev -- build

controller: $(CONTROLLER_FILES)
	cd $(LOCUTUS_DIR)
	cd apps/freenet-microblogging
	cd $(CONTROLLER_DIR)

	CARGO_TARGET_DIR=$(LOCUTUS_DIR)/target/
	bash compile_contract.sh
	mv $(LOCUTUS_DIR)/target/freenet_microblogging_controller.wasm ../../../crates/http-gw/examples/

	cd $(LOCUTUS_DIR)
	cd crates/locutus-dev
	cargo run --bin build_state -- --input-path ../../apps/freenet-microblogging/controllers/initial_state.json --output-file ../http-gw/examples/freenet_microblogging_controller --contract-type controllers

build: controller view

run: build
	cargo run --example contract_browsing --features local

insert_controllers_key_into_web:
	bash insert_controllers_key_into_web.sh 
