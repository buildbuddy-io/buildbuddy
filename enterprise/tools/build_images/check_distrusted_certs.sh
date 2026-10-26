#!/usr/bin/env bash
# Checks that an image's system trust store contains none of the certificates
# listed below, for each platform of the image.
#
# Usage:
#   ./enterprise/tools/build_images/check_distrusted_certs.sh IMAGE [PLATFORM...]
#
# PLATFORM defaults to linux/amd64 and linux/arm64. The container CLI defaults
# to docker, falling back to Apple's `container`; set CONTAINER_CLI to override.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 IMAGE [PLATFORM...]" >&2
  exit 2
fi
IMAGE="$1"
shift
PLATFORMS=("$@")
if [[ ${#PLATFORMS[@]} -eq 0 ]]; then
  PLATFORMS=(linux/amd64 linux/arm64)
fi

if [[ -z "${CONTAINER_CLI:-}" ]]; then
  if command -v docker >/dev/null; then
    CONTAINER_CLI=docker
  else
    CONTAINER_CLI=container
  fi
fi

# SHA-256 fingerprints of certificates that must not be trusted.
DISTRUSTED=(
  # eMudhra intermediates with recoverable private keys, and the roots they
  # chain to (https://emudhra.we-have-a-problem.com/roots.json and
  # https://emudhra.we-have-a-problem.com/impacted-intermediates.json).
  7DD78D5F4F13459A83DFF9ABBBA62EDBAF6F2D102BF257FD712F4D9F2746ED8D # emSign Root TLS CA - G3
  F2EDAAFF3D9B701FB4D14C9F285F8C0BAD90FC3BCC1FA07D47F4718EDABA34A5 # emSign Root CS CA - G3
  D7A7A0FB5D7E2731D771E9484EBCDEF71D5F0C3E0A2948782BC83EE0EA699EF4 # AAA Certificate Services
  86A1ECBA089C4A8D3BBE2734C612BA341D813E043CF9E8A862CD5C57A36BBE6B # emSign ECC Root CA - G3
  42C4ACCD12C4F59597925E1A08A7A90102608543809308D50EC427E118BA1493 # EM DV TLS CA - G3B (issued by emSign Root TLS CA - G3)
  45A883F349AEDFD5F3A649B5498FFB121712BA8B448F3F2A91DBC5A766FCDDBE # EM DV TLS CA - G3B (issued by AAA Certificate Services)
  98B896625C0C230ADEEC3EB2CF6AAD5001ECF9156A68644E82374CE5833BC25B # EM OV TLS CA - G3B (issued by emSign Root TLS CA - G3)
  5F1DED4DD590AF74631290CF073B122A2AAC6A1FA479778A0BD43579BD832699 # EM OV TLS CA - G3B (issued by AAA Certificate Services)
  E7568FE6E12331F4FD7DE61DA4C7559D0F8EB192A218D4741A3BA75184333838 # EM EV TLS CA - G3B (issued by emSign Root TLS CA - G3)
  579480AEA166290BD3C643589E795FC0D0E54E86505D180F1DFF99EFD3199512 # EM EV TLS CA - G3B (issued by AAA Certificate Services)
  0164D7CEAE5BB909C272AF151FF44B1177AE9B47D9424B94F1D9558C30934D19 # EM DV TLS CA - G3C (issued by emSign Root TLS CA - G3)
  62B7F9868263F72F6911FC78CEAF72C1979158B7368BF648683BB2AA39B02E72 # EM DV TLS CA - G3C (issued by AAA Certificate Services)
  CB6E15165AC802E9B3F993F16C25D41F4E81C21EBDE71AE1B012AC00BF9C7C3F # EM OV TLS CA - G3C (issued by emSign Root TLS CA - G3)
  7485B51C0ED192878FA1D2F948FB529AE1842CC5086C4B67F7BE5B2039DE9489 # EM OV TLS CA - G3C (issued by AAA Certificate Services)
  432EC25F0D28DE9006D97BF752F17BBB2CD41F669933FCB072D9A1148D0261F3 # EM EV TLS CA - G3C (issued by emSign Root TLS CA - G3)
  FD9D5C2DBBD4995AB69B8D50C439D9527B00529199C2FD01B2174EB58F07021F # EM EV TLS CA - G3C (issued by AAA Certificate Services)
  9E39DE2881273ACC225A149C72BA3E0F51424963669AB633EE1DC7DF5DB067B4 # EM OV CS CA - G3B (issued by emSign Root CS CA - G3)
  A496E23AC0CAB8155FC9B117B97EAE368EFCBB9041AEC8804D5A308D2D01C2D8 # EM OV CS CA - G3B (issued by AAA Certificate Services)
  4B2460A466F95FD40113BCDCCA2CEB29B3B093921DE45474DA7DF48874C630B1 # EM OV CS CA - G3C (issued by emSign Root CS CA - G3)
  84112972B8E59D14560AE7E7119654474AAD119990CF3722BFC1E3B91615E1FD # EM OV CS CA - G3C (issued by AAA Certificate Services)
)

# Runs inside the image: prints "<fingerprint> <file> <subject>" for every
# certificate in the bundle and in each file under /etc/ssl/certs.
read -r -d '' LIST_CERTS <<'EOF' || true
set -euo pipefail
tmp=$(mktemp -d)
n=0
bundle=/etc/ssl/certs/ca-certificates.crt
for f in "$bundle" /etc/ssl/certs/*; do
  [[ -f "$f" ]] || continue
  [[ "$f" == "$bundle" && "$n" -gt 0 ]] && continue
  awk -v dir="$tmp" -v base="$n" '
    /-----BEGIN CERTIFICATE-----/ { i++; out = sprintf("%s/%d_%d.pem", dir, base, i) }
    out != "" { print > out }
    /-----END CERTIFICATE-----/ { close(out); out = "" }
  ' "$f"
  for c in "$tmp/${n}"_*.pem; do
    [[ -f "$c" ]] || continue
    fp=$(openssl x509 -in "$c" -noout -fingerprint -sha256 | sed 's/.*=//; s/://g')
    subj=$(openssl x509 -in "$c" -noout -subject -nameopt oneline | sed 's/^subject=//')
    echo "$fp $f $subj"
  done
  n=$((n + 1))
done
rm -rf "$tmp"
EOF

status=0
for platform in "${PLATFORMS[@]}"; do
  echo "=== $IMAGE ($platform)"
  certs=$("$CONTAINER_CLI" run --rm --platform "$platform" "$IMAGE" bash -c "$LIST_CERTS")
  bundle_count=$(grep -c ' /etc/ssl/certs/ca-certificates.crt ' <<<"$certs" || true)
  if [[ "$bundle_count" -eq 0 ]]; then
    echo "FAIL: no certificates found in /etc/ssl/certs/ca-certificates.crt"
    status=1
    continue
  fi
  echo "ca-certificates.crt contains $bundle_count certificates"
  found=0
  for fp in "${DISTRUSTED[@]}"; do
    if matches=$(grep "^$fp " <<<"$certs"); then
      echo "FAIL: distrusted certificate is trusted:"
      sed 's/^/  /' <<<"$matches"
      found=1
    fi
  done
  if [[ "$found" -eq 0 ]]; then
    echo "OK: none of the ${#DISTRUSTED[@]} distrusted certificates are trusted"
  else
    status=1
  fi
done
exit "$status"
