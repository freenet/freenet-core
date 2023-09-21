# expected make version >= 3.82

.ONESHELL:

FREENET_DIR := $(abspath ../../)
WEB_DIR := $(FREENET_DIR)/apps/freenet-microblogging/web
POSTS_DIR := $(FREENET_DIR)/apps/freenet-microblogging/contracts/posts

ifeq ($(CARGO_TARGET_DIR),)
$(error CARGO_TARGET_DIR is not set)
endif

build: \
	webapp \
	posts \
	publish-webapp \
	publish-posts

node: \
	build-tool \
	run-node

build-tool:
	cd $(FREENET_DIR)/crates/core &&
	cargo install --path --force $(FREENET_DIR)/crates/core
	cargo install --path --force $(FREENET_DIR)/crates/fdev

webapp:
	cd $(WEB_DIR)
	npm i --force freenet-stdlib
	npm run build

	fdev build
	cp ./build/freenet/freenet_microblogging_web ../../../crates/core/examples/
	cp ./build/freenet/contract-state ../../../crates/core/examples/freenet_microblogging_web_state

publish-webapp:
	cd $(WEB_DIR)

	fdev publish --code build/freenet/freenet_microblogging_web contract --state build/freenet/contract-state

posts:
	cd $(POSTS_DIR)

	fdev build
	cp ./build/freenet/freenet_microblogging_posts ../../../../crates/core/examples/
	cp ./build/freenet/contract-state ../../../../crates/core/examples/freenet_microblogging_posts_state

publish-posts:
	cd $(POSTS_DIR)

	fdev publish --code build/freenet/freenet_microblogging_posts contract --state build/freenet/contract-state

run-node:
	RUST_BACKTRACE=1 RUST_LOG=freenet=debug,locutus_core=debug,locutus_node=debug,info freenet local

run:
	cd $(FREENET_DIR)
	cd crates/core/examples
	cargo run --example contract_browsing
