#!/usr/bin/env bash
PWD=$(pwd)
SRCDIR=${PROJECT_SRC_DIR:-$PWD}
CONTRACTDIR=${CONTRACT_SRC_DIR:-$PWD}
echo "PROJECT_SRC_DIR=${SRCDIR} <- Root of the Project being build"
echo "CONTRACT_SRC_DIR=${CONTRACTDIR}  <- Relative DIR under PROJECT_SRC_DIR to the Contract to build"
docker run -it --rm --env CARGO_TARGET_DIR="/src" -v ${SRCDIR}:/src -v /tmp/freenet-docker:/root/.local/share/freenet -w /src/${CONTRACT_SRC_DIR} freenet:docker fdev $@
