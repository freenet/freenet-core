# expected make version >= 3.82

.ONESHELL:

VIEW_DIR =./view
VIEW_FILES=$(wildcard ./view/*)

MODEL_DIR =./model
MODEL_FILES=$(wildcard ./model/*)

LOCUTUS_DIR=$(shell cd ../.. && pwd)

view: $(VIEW_FILES)
	cd $(LOCUTUS_DIR)
	cd contracts/freenet-microblogging
	cd $(VIEW_DIR)

	CARGO_TARGET_DIR=./ bash compile_contract.sh
	mv ./freenet_microblogging_view.wasm ../../../crates/http-gw/examples/

	cd $(LOCUTUS_DIR)
	cd crates/locutus-dev
	cargo run --bin build_state -- --input-path ../../contracts/freenet-microblogging/view/web --output-file ../http-gw/examples/freenet_microblogging_view --contract-type view

model: $(MODEL_FILES)
	cd $(LOCUTUS_DIR)
	cd contracts/freenet-microblogging
	cd $(MODEL_DIR)

	CARGO_TARGET_DIR=./ bash compile_contract.sh
	mv ./freenet_microblogging_model.wasm ../../../crates/http-gw/examples/

	cd $(LOCUTUS_DIR)
	cd crates/locutus-dev
	cargo run --bin build_state -- --input-path ../../contracts/freenet-microblogging/model/initial_state.json --output-file ../http-gw/examples/freenet_microblogging_model --contract-type model

build: model view

run: build
	cargo run --example contract_browsing --features local

insert_model_key_into_web:
	bash insert_model_key_into_web.sh 
