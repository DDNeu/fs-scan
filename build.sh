#!/bin/sh

ORIGIN=$(pwd)
BIN_DEST="/tmp/fs-scan-bin"

cargo build --release --bins --target x86_64-unknown-linux-musl

cargo build --release --bins --target x86_64-pc-windows-gnu

mkdir -p "${BIN_DEST}"
cp target/x86_64-unknown-linux-musl/release/fs-scan "${BIN_DEST}/fs-scan.bin"
cp target/x86_64-pc-windows-gnu/release/fs-scan.exe "${BIN_DEST}/"

cd ${BIN_DEST}

md5sum fs-scan.* > "signature-fs-scan.md5"
b3sum fs-scan.* > "signature-fs-scan.b3sum"
sha256sum fs-scan.* > "signature-fs-scan.sha256"
sha1sum fs-scan.* > "signature-fs-scan.sha1"

tar -cvzf "${ORIGIN}/fs-scan-binary.tgz" *

rm -rf "${BIN_DEST}"
