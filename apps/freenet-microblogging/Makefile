# expected make version >= 3.82

.ONESHELL:

WEB_DIR =./web
POSTS_DIR =./contracts/posts
LOCUTUS_DIR=$(shell cd ../.. && pwd)

build-tool:
	cd $(LOCUTUS_DIR)
	cd crates/locutus-dev
	cargo build --bin locutus-dev

web:
	cd $(LOCUTUS_DIR)
	cd apps/freenet-microblogging
	cd $(WEB_DIR)

	${CARGO_TARGET_DIR}/debug/locutus-dev build
	mv ./build/locutus/freenet_microblogging_web.wasm ../../../crates/http-gw/examples/
	mv ./build/locutus/contract-state ../../../crates/http-gw/examples/freenet_microblogging_web

posts:
	cd $(LOCUTUS_DIR)
	cd apps/freenet-microblogging
	cd $(POSTS_DIR)

	${CARGO_TARGET_DIR}/debug/locutus-dev build
	mv ./build/locutus/freenet_microblogging_posts.wasm ../../../../crates/http-gw/examples/
	mv ./build/locutus/contract-state ../../../../crates/http-gw/examples/freenet_microblogging_posts

build: build-tool web posts

run: build
	cd $(LOCUTUS_DIR)
	cd crates/http-gw/examples
	cargo run --example contract_browsing --features local
