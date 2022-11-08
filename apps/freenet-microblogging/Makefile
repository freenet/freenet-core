# expected make version >= 3.82

.ONESHELL:

WEB_DIR =./web
POSTS_DIR =./contracts/posts
LOCUTUS_DIR=$(shell cd ../.. && pwd)
CARGO_TARGET_DIR ?= ${LOCUTUS_DIR}/target
export CARGO_TARGET_DIR

build-tool:
	cd $(LOCUTUS_DIR)
	cd crates/locutus-node
	cargo build --bin ldt

webapp:
	cd $(LOCUTUS_DIR)
	cd apps/freenet-microblogging
	cd $(WEB_DIR)

	${CARGO_TARGET_DIR}/debug/ldt build
	mv ./build/locutus/freenet_microblogging_web.wasm ../../../crates/locutus-node/examples/
	mv ./build/locutus/contract-state ../../../crates/locutus-node/examples/freenet_microblogging_web

posts:
	cd $(LOCUTUS_DIR)
	cd apps/freenet-microblogging
	cd $(POSTS_DIR)

	${CARGO_TARGET_DIR}/debug/ldt build
	mv ./build/locutus/freenet_microblogging_posts.wasm ../../../../crates/locutus-node/examples/
	mv ./build/locutus/contract-state ../../../../crates/locutus-node/examples/freenet_microblogging_posts

build: build-tool webapp posts

run:
	cd $(LOCUTUS_DIR)
	cd crates/locutus-node/examples
	cargo run --example contract_browsing 
