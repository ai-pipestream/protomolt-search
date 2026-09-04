#!/usr/bin/env bash
#
# Certificates and secrets for a fleet on mTLS (docs/security.md).
#
# Usage: mkcerts.sh <out dir> <host>=<ip>[,<ip>...] ...
#
#   mkcerts.sh ~/protomolt-search/tls krick-1=192.168.1.195 pi5v3=192.168.1.236
#
# Writes, under <out dir>:
#
#   ca.pem, ca.key          the cluster CA (keep the key on the operator box)
#   <host>.pem, <host>.key  a server identity per host: SANs are the host
#                           name, its addresses, and 127.0.0.1 (the runbook
#                           probes the local coordinator over loopback)
#   client.pem, client.key  the cluster-internal client identity the
#                           coordinator and the tools present to node
#                           listeners
#   udp.key                 the key the floor lane signs datagrams with
#   principals.toml         one public principal ("tools") for the
#                           coordinator's --bearer-tokens
#   bearer.token            that principal's token, for --bearer-token-file
#
# Keys are P-256, PKCS#8; certificates are valid for ten years. Nothing
# here rotates: a new set is a new run and a fleet restart. Existing
# files are kept, so a second run adds a host without reissuing the CA.
set -euo pipefail

out=${1:?out dir}
shift
(($# > 0)) || { echo "mkcerts: at least one <host>=<ip> is needed" >&2; exit 1; }
mkdir -p "$out"
chmod 700 "$out"
days=3650

key() { [[ -f $1 ]] || openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null; chmod 600 "$1"; }

# --- CA ---------------------------------------------------------------
key "$out/ca.key"
if [[ ! -f $out/ca.pem ]]; then
  openssl req -x509 -new -key "$out/ca.key" -days "$days" -subj "/CN=protomolt-search cluster CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$out/ca.pem" 2>/dev/null
fi

# Issue a leaf: sign <name> with the CA under the extensions in $3.
issue() {
  local name=$1 subject=$2 ext=$3
  key "$out/$name.key"
  [[ -f $out/$name.pem ]] && return
  local csr="$out/$name.csr" extfile="$out/$name.ext"
  openssl req -new -key "$out/$name.key" -subj "$subject" -out "$csr" 2>/dev/null
  printf '%s\n' "$ext" >"$extfile"
  openssl x509 -req -in "$csr" -CA "$out/ca.pem" -CAkey "$out/ca.key" -CAcreateserial \
    -days "$days" -extfile "$extfile" -out "$out/$name.pem" 2>/dev/null
  rm -f "$csr" "$extfile"
}

# --- servers ----------------------------------------------------------
for spec in "$@"; do
  host=${spec%%=*}
  ips=${spec#*=}
  [[ $host != "$spec" && -n $ips ]] || { echo "mkcerts: $spec is not <host>=<ip>[,<ip>...]" >&2; exit 1; }
  san="DNS:$host,IP:127.0.0.1"
  IFS=, read -r -a list <<<"$ips"
  for ip in "${list[@]}"; do [[ $ip == 127.0.0.1 ]] || san="$san,IP:$ip"; done
  issue "$host" "/CN=$host" "basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
subjectAltName=$san"
done

# --- the cluster-internal client identity ------------------------------
issue client "/CN=protomolt-search coordinator" "basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=clientAuth"

# --- secrets ----------------------------------------------------------
if [[ ! -f $out/udp.key ]]; then
  openssl rand -hex 32 >"$out/udp.key"
  chmod 600 "$out/udp.key"
fi
if [[ ! -f $out/bearer.token ]]; then
  openssl rand -hex 32 >"$out/bearer.token"
  chmod 600 "$out/bearer.token"
fi
if [[ ! -f $out/principals.toml ]]; then
  cat >"$out/principals.toml" <<TOML
# The coordinator's public principals (docs/security.md). Quotas of 0
# mean no limit of that kind; the verifier and the sweeps ask for k up
# to 1000.
[[principals]]
name = "tools"
token = "$(cat "$out/bearer.token")"
max_k = 0
concurrency = 0
ingest_docs_per_sec = 0
TOML
  chmod 600 "$out/principals.toml"
fi
rm -f "$out/ca.srl"

echo "mkcerts: $out"
for f in "$out"/*.pem; do
  printf '  %-14s %s\n' "$(basename "$f")" "$(openssl x509 -in "$f" -noout -subject -ext subjectAltName 2>/dev/null | tr '\n' ' ' | sed 's/  */ /g')"
done
