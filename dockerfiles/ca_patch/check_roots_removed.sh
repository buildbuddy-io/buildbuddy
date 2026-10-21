#!/bin/sh
# Fails unless every given /etc/ca-certificates.conf entry is no longer
# trusted: not selected, no .pem link in /etc/ssl/certs, not in the bundle (by
# SHA-256 fingerprint), not in the Debian Java keystore; and no dangling links
# in /etc/ssl/certs. Used by both Dockerfiles (bind-mounted, so it never lands
# in the image) and by validate.sh.
#   check_roots_removed.sh mozilla/emSign_ECC_Root_CA_-_G3.crt ...
set -eu

[ $# -gt 0 ] || { echo "check_roots_removed.sh: no certs given" >&2; exit 1; }

fingerprint() {
  case "$1" in
    mozilla/emSign_ECC_Root_CA_-_G3.crt) echo 86:A1:EC:BA:08:9C:4A:8D:3B:BE:27:34:C6:12:BA:34:1D:81:3E:04:3C:F9:E8:A8:62:CD:5C:57:A3:6B:BE:6B ;;
    mozilla/Comodo_AAA_Services_root.crt) echo D7:A7:A0:FB:5D:7E:27:31:D7:71:E9:48:4E:BC:DE:F7:1D:5F:0C:3E:0A:29:48:78:2B:C8:3E:E0:EA:69:9E:F4 ;;
    *) echo "unknown cert $1" >&2; return 1 ;;
  esac
}

failed=0
fail() { echo "FAIL: $*" >&2; failed=1; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
awk -v d="$tmp" '/BEGIN CERTIFICATE/ { if (f) close(f); f = d "/" (++n) ".pem" } f { print > f }' /etc/ssl/certs/ca-certificates.crt
for f in "$tmp"/*.pem; do
  openssl x509 -in "$f" -noout -fingerprint -sha256 | cut -d= -f2
done >"$tmp/fps"
[ -s "$tmp/fps" ] || fail "no certificates in /etc/ssl/certs/ca-certificates.crt"
echo "bundle has $(wc -l <"$tmp/fps") certificates"

jks=/etc/ssl/certs/java/cacerts
if [ -e "$jks" ]; then
  if command -v keytool >/dev/null 2>&1; then
    keytool -list -v -keystore "$jks" -storepass changeit >"$tmp/jks" 2>&1 || fail "keytool cannot list $jks"
    grep -q '^Alias name:' "$tmp/jks" || fail "keytool listed no entries in $jks"
    echo "$jks has $(grep -c '^Alias name:' "$tmp/jks") entries"
  else
    echo "WARNING: $jks exists but keytool is not available; not checked" >&2
  fi
fi

for c in "$@"; do
  fp=$(fingerprint "$c")
  name=$(basename "$c" .crt)
  ! grep -qxF "$c" /etc/ca-certificates.conf || fail "$c is still selected in /etc/ca-certificates.conf"
  [ ! -e "/etc/ssl/certs/$name.pem" ] && [ ! -L "/etc/ssl/certs/$name.pem" ] || fail "/etc/ssl/certs/$name.pem still present"
  ! grep -qxF "$fp" "$tmp/fps" || fail "$c still in ca-certificates.crt"
  if [ -s "$tmp/jks" ]; then
    ! grep -qF "$fp" "$tmp/jks" || fail "$c still in $jks"
  fi
  echo "checked $c"
done

dangling=$(find -L /etc/ssl/certs -maxdepth 1 -type l)
[ -z "$dangling" ] || fail "dangling symlinks in /etc/ssl/certs: $dangling"

exit "$failed"
