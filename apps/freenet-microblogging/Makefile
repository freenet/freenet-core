# expected make version >= 3.82

.ONESHELL:

LOCUTUS_DIR := $(abspath ../../)
WEB_DIR := $(LOCUTUS_DIR)/apps/freenet-microblogging/web
POSTS_DIR := $(LOCUTUS_DIR)/apps/freenet-microblogging/contracts/posts

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

build-app: \
	webapp \
	posts \
	publish-webapp \
	publish-posts

node: \
	build-tool \
	run-node

build-tool:
	cd $(LOCUTUS_DIR)/crates/locutus-node &&
	cargo install --path $(LOCUTUS_DIR)/crates/locutus-node

webapp:
	cd $(WEB_DIR)
	npm run build

	ldt build
	cp ./build/locutus/freenet_microblogging_web ../../../crates/locutus-node/examples/
	cp ./build/locutus/contract-state ../../../crates/locutus-node/examples/freenet_microblogging_web_state

publish-webapp:
	cd $(WEB_DIR)

	ldt publish --code build/locutus/freenet_microblogging_web contract --state build/locutus/contract-state

posts:
	cd $(POSTS_DIR)

	ldt build
	cp ./build/locutus/freenet_microblogging_posts ../../../../crates/locutus-node/examples/
	cp ./build/locutus/contract-state ../../../../crates/locutus-node/examples/freenet_microblogging_posts_state

publish-posts:
	cd $(POSTS_DIR)

	ldt publish --code build/locutus/freenet_microblogging_posts contract --state build/locutus/contract-state

run-node:
	RUST_BACKTRACE=1 RUST_LOG=locutus=debug,locutus_core=debug,locutus_node=debug,info locutus-node local

run:
	cd $(LOCUTUS_DIR)
	cd crates/locutus-node/examples
	cargo run --example contract_browsing
