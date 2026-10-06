#!/usr/bin/env bash
# Regenerates the fixtures for crates/core/tests/tls_native_roots.rs:
#   ca.pem        throwaway CA, installed in the test via SSL_CERT_FILE
#   leaf.der      server cert for localhost / 127.0.0.1, signed by the CA
#   leaf.key.der  the server cert's PKCS#8 private key (test-only, not a secret)
#
# Validity is 2020-01-01 to 2126-01-01: backdated so a runner whose clock is
# behind cannot see the certs as not yet valid. Keys are random, so commit all
# three files together after regenerating.
set -euo pipefail

out="$(cd "$(dirname "$0")" && pwd)"
work="$(mktemp -d)"
trap 'rm -r "$work"' EXIT
cd "$work"

mkdir newcerts
touch index.txt
echo 1000 > serial
cat > ca.cnf <<'EOF'
[ca]
default_ca = test_ca

[test_ca]
dir = .
database = index.txt
new_certs_dir = newcerts
serial = serial
default_md = sha256
policy = any
unique_subject = no
copy_extensions = none

[any]
commonName = supplied

[ca_ext]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash

[leaf_ext]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,IP:127.0.0.1
authorityKeyIdentifier = keyid
EOF

dates=(-startdate 20200101000000Z -enddate 21260101000000Z)

openssl ecparam -name prime256v1 -genkey -noout -out ca.key
openssl req -new -key ca.key -subj "/CN=freenet-test-intercepting-ca" -out ca.csr
openssl ca -batch -config ca.cnf -selfsign -keyfile ca.key -in ca.csr \
  -extensions ca_ext "${dates[@]}" -notext -out ca.pem

openssl ecparam -name prime256v1 -genkey -noout -out leaf.key
openssl req -new -key leaf.key -subj "/CN=localhost" -out leaf.csr
openssl ca -batch -config ca.cnf -cert ca.pem -keyfile ca.key -in leaf.csr \
  -extensions leaf_ext "${dates[@]}" -notext -out leaf.pem

cp ca.pem "$out/ca.pem"
openssl x509 -in leaf.pem -outform DER -out "$out/leaf.der"
openssl pkcs8 -topk8 -nocrypt -in leaf.key -outform DER -out "$out/leaf.key.der"
