#!/usr/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR && cd ..
SRC_DIR=$(pwd)

cargo publish --dry-run -p freenet || { exit 1; }
read -p "Publish freenet? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p freenet
else 
	echo "Not publishing freenet"
fi

cargo publish --dry-run -p fdev || { exit 1; }
read -p "Publish fdev? " -n 1 -r
echo   
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cargo publish -p fdev
else 
	echo "Not publishing fdev"
fi
