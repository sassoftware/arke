#!/bin/bash

# https://www.rabbitmq.com/ssl.html#manual-certificate-generation

additional_hostnames=$*

sans=""

for hn in $additional_hostnames; do
	# shellcheck disable=2268
	if [[ "x$hn" != "x" ]]; then
		sans="${sans},DNS:$hn"
	fi
done

set -x
set -e

rm -rf /certs/*
# mkdir /certs
cd /certs

mkdir testca
cd testca
mkdir certs private
chmod 700 private
echo 01 >serial
touch index.txt

cat <<EOF >>openssl.cnf

[ ca ]
default_ca = testca

[ testca ]
dir = .
certificate = \$dir/ca_certificate.pem
database = \$dir/index.txt
new_certs_dir = \$dir/certs
private_key = \$dir/private/ca_private_key.pem
serial = \$dir/serial
copy_extensions = copy

default_crl_days = 7
default_days = 365
default_md = sha256

policy = testca_policy
x509_extensions = certificate_extensions

[ testca_policy ]
commonName = supplied
stateOrProvinceName = optional
countryName = optional
emailAddress = optional
organizationName = optional
organizationalUnitName = optional
domainComponent = optional

[ certificate_extensions ]
basicConstraints = CA:false

[ req ]
default_bits = 2048
default_keyfile = ./private/ca_private_key.pem
default_md = sha256
prompt = yes
distinguished_name = root_ca_distinguished_name
x509_extensions = root_ca_extensions

[ root_ca_distinguished_name ]
commonName = hostname

[ root_ca_extensions ]
basicConstraints = CA:true
keyUsage = keyCertSign, cRLSign

[ client_ca_extensions ]
basicConstraints = CA:false
keyUsage = digitalSignature,keyEncipherment
extendedKeyUsage = 1.3.6.1.5.5.7.3.2

[ server_ca_extensions ]
basicConstraints = CA:false
keyUsage = digitalSignature,keyEncipherment
extendedKeyUsage = 1.3.6.1.5.5.7.3.1

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment

EOF

function generateCert() {
	local svc=$1
	openssl genrsa -out "${svc}.key" 2048
	openssl req -new -key "${svc}.key" -out "${svc}_req.csr" -outform PEM \
		-subj "/CN=${svc}/O=server/" -nodes \
		-addext "subjectAltName=DNS:localhost,DNS:${svc}${sans}"
	cd ../testca
	openssl ca -config openssl.cnf -in "../server/${svc}_req.csr" -out \
		"../server/${svc}.pem" -notext -batch -extensions server_ca_extensions
	cd ../server
	openssl pkcs12 -export -out "${svc}.p12" -in "${svc}.pem" -inkey "${svc}.key" \
		-passout pass:
}

openssl req -x509 -config openssl.cnf -newkey rsa:2048 -days 365 \
	-out ca_certificate.pem -outform PEM -subj /CN=MyTestCA/ -nodes
openssl x509 -in ca_certificate.pem -out ca_certificate.cer -outform DER

cd ..
mkdir server
cd server

generateCert "rabbitmq"
generateCert "arke"

cat rabbitmq.pem rabbitmq.key >rabbitmq_combined.pem

cd ../

chmod -R 755 testca
chmod -R 755 server

cd ../
