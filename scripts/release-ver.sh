#!/usr/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR && cd ..
SRC_DIR=$(pwd)
source apps/stdlib/package.sh
cd $SRC_DIR

cargo publish --dry-run -p locutus-macros || { exit 1; }
read -p "Publish locutus-macros? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p locutus-macros
else 
	echo "Not publishing locutus-macros"
fi

cargo publish --dry-run -p locutus-stdlib || { exit 1; }
read -p "Publish locutus-stdlib? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p locutus-stdlib
else 
	echo "Not publishing locutus-stdlib"
fi

cargo publish --dry-run -p locutus-runtime || { exit 1; }
read -p "Publish locutus-runtime? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p locutus-runtime
else 
	echo "Not publishing locutus-runtime"
fi

cargo publish --dry-run -p locutus-core || { exit 1; }
read -p "Publish locutus-core? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p locutus-core
else 
	echo "Not publishing locutus-core"
fi

cargo publish --dry-run -p locutus-dev || { exit 1; }
read -p "Publish locutus-dev? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p locutus-dev
else 
	echo "Not publishing locutus-dev"
fi

cargo publish --dry-run -p locutus || { exit 1; }
read -p "Publish locutus? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p locutus
else 
	echo "Not publishing locutus"
fi
