# expected make version >= 3.82

.ONESHELL:

INBOX_DIR=contracts/inbox
INBOX_SRC_DIR=contracts/inbox/src
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

	${CARGO_TARGET_DIR}/debug/ldt build --features contract

	cd $(INBOX_SRC_DIR)

	# Generate contract parameters
	cargo run -- 1 "../../../web/examples/rsa4096-id-0-priv.pem"

	cd ..

	${CARGO_TARGET_DIR}/debug/ldt publish --code build/locutus/freenet_email_inbox --state build/locutus/contract-state --parameters inbox_key_1

build-and-publish-inbox-2:
	cd $(LOCUTUS_DIR)
	cd apps/freenet-email-app
	cd $(INBOX_DIR)

	${CARGO_TARGET_DIR}/debug/ldt build --features contract

	cd $(INBOX_SRC_DIR)

	# Generate contract parameters
	cargo run -- 1 "../../../web/examples/rsa4096-id-1-priv.pem"

	cd ..

	${CARGO_TARGET_DIR}/debug/ldt publish --code build/locutus/freenet_email_inbox --state build/locutus/contract-state --parameters inbox_key_2

build: build-tool build-and-publish-inbox-1 build-and-publish-inbox-2
