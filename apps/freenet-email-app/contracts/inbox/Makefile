# expected make version >= 3.82

.ONESHELL:

INBOX_DIR =contracts/inbox
LOCUTUS_DIR=$(shell cd ../../../../ && pwd)
CARGO_TARGET_DIR ?= ${LOCUTUS_DIR}/target
export CARGO_TARGET_DIR

build-tool:
	cd $(LOCUTUS_DIR)
	cd crates/locutus-node
	cargo build --bin ldt

build-and-publish-inbox-1:
	cd $(LOCUTUS_DIR)
	cd apps/freenet-email-app
	cd $(INBOX_DIR)

	${CARGO_TARGET_DIR}/debug/ldt build

	${CARGO_TARGET_DIR}/debug/ldt publish --code build/locutus/freenet_email_inbox --state build/locutus/contract-state --parameters ../../web/examples/rsa4096-pub.pem

build-and-publish-inbox-2:
	cd $(LOCUTUS_DIR)
	cd apps/freenet-email-app
	cd $(POSTS_DIR)

	${CARGO_TARGET_DIR}/debug/ldt build

	${CARGO_TARGET_DIR}/debug/ldt publish --code build/locutus/freenet_email_inbox --state build/locutus/contract-state --parameters ../../web/examples/rsa2048-pub.pem

build: build-tool build-and-publish-inbox-1 build-and-publish-inbox-2
