---
id: config-ssl
title: SSL Configuration
sidebar_label: SSL
---

## Section

`ssl:` The SSL section enables SSL/TLS on build event protocol and remote cache gRPC connections (gRPCS). **Optional**

## Options

**Optional**

- `enable_ssl:` Whether or not to enable SSL/TLS on gRPC connections (gRPCS).

- `use_acme:` Whether or not to automatically configure SSL certs using [ACME](https://en.wikipedia.org/wiki/Automated_Certificate_Management_Environment). If ACME is enabled, cert_file and key_file should not be set.

- `cert_file:` Path to a PEM encoded certificate file to use for TLS if not using ACME.

- `key_file:` Path to a PEM encoded key file to use for TLS if not using ACME.

- `client_ca_cert_file:` Path to a PEM encoded certificate authority file used to issue client certificates for mTLS auth.

- `client_ca_key_file:` Path to a PEM encoded certificate authority key file used to issue client certificates for mTLS auth.

## Generating client CA files

```
# Change these CN's to match your BuildBuddy host name
SERVER_SUBJECT=buildbuddy.io
PASS=$(openssl rand -base64 32) # <- Save this :)

# Generates ca.key
openssl genrsa -passout pass:${PASS} -des3 -out ca.key 4096

# Generates ca.crt
openssl req -passin pass:${PASS} -new -x509 -days 365000 -key ca.key -out ca.crt -subj "/CN=${SERVER_SUBJECT}"

# Generates ca.pem
openssl pkcs8 -passin pass:${PASS} -topk8 -nocrypt -in ca.key -out ca.pem

```

## Example section

```
ssl:
  enable_ssl: true
  use_acme: true
  client_ca_cert_file: your_ca.crt
  client_ca_key_file: your_ca.pem
```
