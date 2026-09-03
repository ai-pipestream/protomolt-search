#!/usr/bin/env bash
# Regenerate the TLS fixtures under tests/certs (docs/security.md).
#
# Two CAs: `ca` signs the server and client identities the tests trust;
# `other-ca` signs `other-client`, the identity the tests expect a
# server to reject. Leaves the CA private keys out of the tree: the
# fixtures are for tests, and a CA key on disk is one thing too many.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../tests/certs"
rm -f ./*.pem ./*.srl
new_ca() {
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$1.key.pem" -out "$1.pem" -days 36500 -subj "/CN=pipestream-test-$1" 2>/dev/null
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
issue server ca
issue client ca
issue other-client other-ca
rm -f ./*.srl ca.key.pem other-ca.key.pem
openssl verify -CAfile ca.pem server.pem client.pem
