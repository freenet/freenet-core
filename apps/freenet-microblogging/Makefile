# expected make version >= 3.82

.ONESHELL:

WEB_DIR =./web
POSTS_DIR =./contracts/posts
LOCUTUS_DIR := $(abspath ../../)

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

build-tool:
	cd $(LOCUTUS_DIR)/crates/locutus-node && cargo build --release
	cargo install --path $(LOCUTUS_DIR)/crates/locutus-node

webapp:
	cd $(LOCUTUS_DIR)
	cd apps/freenet-microblogging
	cd $(WEB_DIR)

	${CARGO_TARGET_DIR}/debug/ldt build
	mv ./build/locutus/freenet_microblogging_web ../../../crates/locutus-node/examples/
	mv ./build/locutus/contract-state ../../../crates/locutus-node/examples/freenet_microblogging_web_state

posts:
	cd $(LOCUTUS_DIR)
	cd apps/freenet-microblogging
	cd $(POSTS_DIR)

	${CARGO_TARGET_DIR}/debug/ldt build
	mv ./build/locutus/freenet_microblogging_posts ../../../../crates/locutus-node/examples/
	mv ./build/locutus/contract-state ../../../../crates/locutus-node/examples/freenet_microblogging_posts_state

build: build-tool webapp posts

run:
	cd $(LOCUTUS_DIR)
	cd crates/locutus-node/examples
	cargo run --example contract_browsing 
