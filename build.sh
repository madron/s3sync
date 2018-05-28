#!/bin/sh

# pip3 install -r requirements-build.txt

rm -rf dist
pyinstaller --onefile -n s3sync main.py
rm -rf build s3sync.spec
