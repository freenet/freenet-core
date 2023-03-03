#!/usr/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
set -e
npm install --save-dev .
npm run test
npm run build
if [ "$1"="dev" ]; then 
PKG_DIR=$SCRIPT_DIR/dist/pack
IS_DEV=1
else
PKG_DIR=${1:-$(mktemp -d)}
fi
mkdir -p  $PKG_DIR
echo "Package dir: $PKG_DIR"
cd $PKG_DIR
cp -r $SCRIPT_DIR/dist/src/* $PKG_DIR
cp $SCRIPT_DIR/README.md $SCRIPT_DIR/package.json $PKG_DIR
npm pack $PKG_DIR 
if [ "$IS_DEV"=1 ]; then
mv $PKG_DIR/*.tgz $PKG_DIR/locutus-stdlib.tgz
fi
