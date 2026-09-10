#!/usr/bin/env bash
# Regenerate the Raft transport TLS fixtures under tests/certs/raft
# (docs/raft-hosting.md). Separate from tests/certs: those fixtures carry
# a fingerprint pinned in a test, and this set needs one identity per
# member.
#
# `ca` issues node-1 .. node-4 (each a server and client identity of one
# member); `other-ca` issues `stranger`, which no listener accepts. CA
# private keys are not kept.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../tests/certs/raft"
rm -f ./*.pem ./*.srl
new_ca() {
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$1.key.pem" -out "$1.pem" -days 36500 -subj "/CN=protomolt-raft-test-$1" 2>/dev/null
}
new_ca ca
new_ca other-ca
issue() {
  local name=$1 ca=$2
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$name.key.pem" -out "$name.csr" -subj "/CN=$name" 2>/dev/null
  printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n' >"$name.ext"
  openssl x509 -req -in "$name.csr" -CA "$ca.pem" -CAkey "$ca.key.pem" -CAcreateserial \
    -out "$name.pem" -days 36500 -extfile "$name.ext" 2>/dev/null
  rm -f "$name.csr" "$name.ext"
}
for n in 1 2 3 4; do issue "node-$n" ca; done
issue stranger other-ca
rm -f ./*.srl ca.key.pem other-ca.key.pem
openssl verify -CAfile ca.pem node-1.pem node-2.pem node-3.pem node-4.pem
