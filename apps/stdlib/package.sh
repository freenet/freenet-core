#!/usr/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PKG_DIR=$(mktemp -d)
cd $PKG_DIR
cp -r $SCRIPT_DIR/src/* $PKG_DIR
cp -r $SCRIPT_DIR/dist/src/* $PKG_DIR
cp $SCRIPT_DIR/README.md $SCRIPT_DIR/package.json $PKG_DIR
npm pack $PKG_DIR
echo "Packages at $PKG_DIR"
OUTPUT=$(find . -type f -name '*.tgz')
echo "Publishing $OUTPUT ..."
read -p "Proceed publishing? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
	npm publish $OUTPUT
else 
	echo "Not publishing"
fi
