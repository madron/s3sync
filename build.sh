#!/bin/sh

# pip3 install -r requirements-build.txt

rm -rf dist
pyinstaller --onefile s3sync.py
rm -rf build s3sync.spec
