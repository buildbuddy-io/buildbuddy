# mocksaml

This package contains a mock SAML identity provider (IDP) that can be
used to test BuildBuddy SAML auth locally.

Instructions:

```bash
# Generate a self-signed cert for the IDP:
openssl req -x509 -newkey rsa:2048 -keyout /tmp/mocksaml.key -out /tmp/mocksaml.crt -sha256 -days 365000 -nodes -subj "/CN=localhost"

# Start the mocksaml IDP server:
bazel run -- enterprise/tools/mocksaml -cert_file=/tmp/mocksaml.crt -key_file=/tmp/mocksaml.key

# Generate a self-signed cert for BuildBuddy:
openssl req -x509 -newkey rsa:2048 -keyout /tmp/bb.key -out /tmp/bb.crt -sha256 -days 365000 -nodes -subj "/CN=localhost"

# Start BuildBuddy enterprise server, configured with the self-signed cert
# as well as the IDP's public key:

bazel run -- enterprise/server -auth.saml.key_file=/tmp/bb.key -auth.saml.cert_file=/tmp/bb.crt -auth.saml.trusted_idp_cert_files=/tmp/mocksaml.crt

# In the local BB UI, log in and set the group's slug to "samltest".
# Then configure the IDP metadata URL in the DB:
sqlite3 /tmp/$USER-buildbuddy-enterprise.db "UPDATE Groups SET saml_idp_metadata_url = 'http://localhost:4000/api/saml/metadata' WHERE url_identifier = 'samltest';"

# Now, to log in to the group using SAML auth, first log out from the
# BB UI if you're already logged in. Then, click on the login URL
# that the mocksaml server printed out to the terminal. From there,
# you can click "Sign in" (with any credentials) and you should now
# be logged in to the samltest org via SAML :)
```
