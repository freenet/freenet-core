#!/usr/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR && cd ..
SRC_DIR=$(pwd)
source apps/stdlib/package.sh
cd $SRC_DIR

cargo publish --dry-run -p freenet-macros || { exit 1; }
read -p "Publish freenet-macros? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p freenet-macros
else 
	echo "Not publishing freenet-macros"
fi

cargo publish --dry-run -p freenet-stdlib || { exit 1; }
read -p "Publish freenet-stdlib? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p freenet-stdlib
else 
	echo "Not publishing freenet-stdlib"
fi

cargo publish --dry-run -p freenet-runtime || { exit 1; }
read -p "Publish freenet-runtime? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p freenet-runtime
else 
	echo "Not publishing freenet-runtime"
fi

cargo publish --dry-run -p freenet-core || { exit 1; }
read -p "Publish freenet-core? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p freenet-core
else 
	echo "Not publishing freenet-core"
fi

cargo publish --dry-run -p freenet-dev || { exit 1; }
read -p "Publish freenet-dev? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p freenet-dev
else 
	echo "Not publishing freenet-dev"
fi

cargo publish --dry-run -p freenet || { exit 1; }
read -p "Publish freenet? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p freenet
else 
	echo "Not publishing freenet"
fi
