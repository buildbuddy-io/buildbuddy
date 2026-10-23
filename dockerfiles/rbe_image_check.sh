#!/usr/bin/env bash
# Smoke-checks the RBE image it runs in. Run it inside the image, e.g. as an
# action on BuildBuddy RBE or with
# `docker run --rm -v "$PWD/dockerfiles:/c:ro" IMAGE /c/rbe_image_check.sh`.
#
# Usage:
#   rbe_image_check.sh
#       Checks that the tools the image's Dockerfile installs are present (and
#       that the ones it downloads outside apt run), the image's locale, user
#       and iptables setup, that https works, and that `git ls-remote` against
#       GitHub works, and that the image doesn't trust the compromised eMudhra
#       CAs (see "CA checks"). The Ubuntu release is read from /etc/os-release.
#       Exits nonzero if anything fails.
#   rbe_image_check.sh --packages
#       Prints "<package> <version>" for every installed dpkg package (status
#       "ii"; removed packages with leftover config files are skipped), sorted,
#       to stdout. Fails if the list is empty.
set -euo pipefail

packages=0
for arg in "$@"; do
  case "$arg" in
    --packages) packages=1 ;;
    *) echo "usage: $0 [--packages]" >&2; exit 2 ;;
  esac
done

if ((packages)); then
  pkgs=$(dpkg-query -W -f='${db:Status-Abbrev} ${Package} ${Version}\n' |
    awk '$1 == "ii" {print $2, $3}' | LC_ALL=C sort)
  if [[ -z "$pkgs" ]]; then
    echo "dpkg-query listed no packages" >&2
    exit 1
  fi
  printf '%s\n' "$pkgs"
  exit 0
fi

family=$(. /etc/os-release && echo "${VERSION_ID:-}")

# Tools each family's Dockerfile (dockerfiles/rbe-ubuntu*-04) installs.
common="gcc g++ make python3 git curl wget ed file less nc unzip zip ssh docker"
# Binaries the Dockerfile downloads outside apt can be built for the wrong
# arch, so those are also run once.
runs=()
setup=0
case "$family" in
  20.04)
    tools="$common python2 python java javac clang go"
    # java and javac print their version to stderr.
    runs=("go version" "clang --version" "java -version 2>&1" "javac -version 2>&1")
    # The Dockerfile adds this with ENV PATH, which RBE actions don't inherit.
    PATH="$PATH:/usr/local/go/bin"
    ;;
  22.04) tools="$common python sudo"; setup=1 ;;
  24.04) tools="$common python sudo cmake"; setup=1 ;;
  *) echo "unsupported family: $family" >&2; exit 2 ;;
esac

# CA checks.
#
# In October 2026 eMudhra intermediate CAs were compromised. The intermediates
# chain to the roots below, so an image must not trust them; patched
# ca-certificates (20260816ubuntu0.* on 22.04 and 24.04) drops them. To
# distrust another root, add a line "<SHA-256 fingerprint> <name>".
distrusted_roots='
86:A1:EC:BA:08:9C:4A:8D:3B:BE:27:34:C6:12:BA:34:1D:81:3E:04:3C:F9:E8:A8:62:CD:5C:57:A3:6B:BE:6B emSign ECC Root CA - G3
D7:A7:A0:FB:5D:7E:27:31:D7:71:E9:48:4E:BC:DE:F7:1D:5F:0C:3E:0A:29:48:78:2B:C8:3E:E0:EA:69:9E:F4 Comodo AAA Certificate Services
'
# The roots are looked for only in the bundle: update-ca-certificates derives
# the hash links in /etc/ssl/certs and the Java keystore from the same
# selection of certificates.
bundle=/etc/ssl/certs/ca-certificates.crt
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# These run inside `if`, where set -e is off, so each step returns on failure
# and the reason is printed to stderr.
distrusted_roots_absent() {
  local fp out f cert n=0 line found=0
  local -A fps=()
  mkdir "$tmp/bundle" || return 1
  awk -v d="$tmp/bundle" '
    /-----BEGIN CERTIFICATE-----/ { f = d "/" ++n ".pem" }
    f { print > f }
    /-----END CERTIFICATE-----/ { if (f) close(f); f = "" }' "$bundle" || return 1
  for f in "$tmp"/bundle/*.pem; do
    cert=${f##*/}
    out=$(openssl x509 -noout -fingerprint -sha256 -in "$f" 2>&1) || { echo "openssl x509 failed on certificate ${cert%.pem} in $bundle: $out" >&2; return 1; }
    # OpenSSL 1.1.1 prints "SHA256 Fingerprint=AB:...", 3.x "sha256 Fingerprint=AB:...".
    fp=${out#*=}
    if [[ ! "$fp" =~ ^([0-9A-F]{2}:){31}[0-9A-F]{2}$ ]]; then
      echo "unexpected openssl x509 output: $out" >&2
      return 1
    fi
    fps[$fp]=1
    n=$((n + 1))
  done
  ((n >= 50)) || { echo "found only $n certificates in $bundle (need at least 50)" >&2; return 1; }
  while read -r fp line; do
    [[ -n "$fp" ]] || continue
    if [[ -n "${fps[$fp]:-}" ]]; then echo "$bundle trusts $line ($fp)" >&2; found=1; fi
  done <<<"$distrusted_roots"
  ((!found))
}

# Chains served by the disclosure's test hosts (leaf and intermediates, never
# the roots), captured with `openssl s_client -showcerts -servername HOST
# -connect HOST:443` on 2026-10-23. The leaves are valid from Oct 20 2026 to
# May 3 2027, so they're verified at a fixed time inside that window.
# dv-g3b-emudhra.we-have-a-problem.com: leaf -> EM DV TLS CA - G3B ->
# emSign Root TLS CA - G3 (cross-signed by emSign ECC Root CA - G3).
cat >"$tmp/emudhra.pem" <<'EOF'
-----BEGIN CERTIFICATE-----
MIICOTCCAb6gAwIBAgIUY1R/PHrtXShV0uhLJ5EyjsHvMi0wCgYIKoZIzj0EAwIw
PzELMAkGA1UEBhMCTkwxEzARBgNVBAoMCmVNdWRocmEgQlYxGzAZBgNVBAMMEkVN
IERWIFRMUyBDQSAtIEczQjAeFw0yNjEwMjAwMDU5MzZaFw0yNzA1MDMwMTA0MzZa
MC8xLTArBgNVBAMMJGR2LWczYi1lbXVkaHJhLndlLWhhdmUtYS1wcm9ibGVtLmNv
bTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABFpE24ACBFHQLzpg+7D767thJ3+Q
NIoJJugUMT/OftEz6G6uDdxlS+9VfXYBDglHjJirjEPFNglOGJoQNQuiz7Gjgacw
gaQwDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8EBAMCB4AwEwYDVR0lBAwwCgYIKwYB
BQUHAwEwLwYDVR0RBCgwJoIkZHYtZzNiLWVtdWRocmEud2UtaGF2ZS1hLXByb2Js
ZW0uY29tMB0GA1UdDgQWBBT4r5v4etYNuG21BqBqs273OkcHiDAfBgNVHSMEGDAW
gBTiHi/nHBk5DlVLYsEgJFc1JIolFDAKBggqhkjOPQQDAgNpADBmAjEAlxNF+/8l
APpgibawACy15yn5bcvTksz2Zw0R9z9nUK9e3MSSk+hXagg7LVb7V4NtAjEA4qyR
laFPkaokrnTjqQieq8CgDQ6tIz3FjBvj3D7vkhKXHaml6zD2zT5ih2Y+lD2h
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIDaDCCAu6gAwIBAgIQAKSu5njyM2K0wORgt3LyFDAKBggqhkjOPQQDAzBWMQsw
CQYDVQQGEwJJTjElMCMGA1UECgwcZU11ZGhyYSBUZWNobm9sb2dpZXMgTGltaXRl
ZDEgMB4GA1UEAwwXZW1TaWduIFJvb3QgVExTIENBIC0gRzMwHhcNMjUwNDMwMTU1
NDA4WhcNNDAwNDI4MTgzMDAwWjA/MQswCQYDVQQGEwJOTDETMBEGA1UECgwKZU11
ZGhyYSBCVjEbMBkGA1UEAwwSRU0gRFYgVExTIENBIC0gRzNCMHYwEAYHKoZIzj0C
AQYFK4EEACIDYgAE557ZwEqXc8RzXlERHahawr1eZKs3S4DhLLwgOOweTw47gS/J
hZ4eZ6+wTi+LyX90hbplqpEe6KTkW2LVzAPVa+Z4s8OMtNyTfKJGDiOEU8iSk10p
1E+R4VYGtjzO2rRoo4IBljCCAZIwHwYDVR0jBBgwFoAU4TqrIXzx401UrtRyt/2L
WUIgkFQwHQYDVR0OBBYEFOIeL+ccGTkOVUtiwSAkVzUkiiUUMB0GA1UdJQQWMBQG
CCsGAQUFBwMBBggrBgEFBQcDAjBdBgNVHSAEVjBUMDkGCysGAQQBg44hAQABMCow
KAYIKwYBBQUHAgEWHGh0dHA6Ly9yZXBvc2l0b3J5LmVtc2lnbi5jb20wDQYLKwYB
BAGDjiEBAmQwCAYGZ4EMAQIBMHUGCCsGAQUFBwEBBGkwZzAkBggrBgEFBQcwAYYY
aHR0cDovL29jc3AtYi5lbXNpZ24uY29tMD8GCCsGAQUFBzAChjNodHRwOi8vcmVw
b3NpdG9yeS5lbXNpZ24uY29tL2NlcnRzL0VNRFZUTFNDQUczQi5wN2MwNwYDVR0f
BDAwLjAsoCqgKIYmaHR0cDovL2NybC5lbXNpZ24uY29tLz9yb290dGxzY2FnMy5j
cmwwDgYDVR0PAQH/BAQDAgEGMBIGA1UdEwEB/wQIMAYBAf8CAQAwCgYIKoZIzj0E
AwMDaAAwZQIwLmsGXi6oFMJ2klkcEMXMug6gvxQWZWkewiDWVzQyl2L6z6DuQfPO
Ibh1grvdULDCAjEAxZvLlxUavG69J6wnSjC8XBbqNIQsJ6CE2QdNOe84Ptxb6VG8
gt2TlfJbHii+KNvK
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIDdDCCAvmgAwIBAgIQAJYaPvsTWVdu0EAFtGhKWzAKBggqhkjOPQQDAzBrMQsw
CQYDVQQGEwJJTjETMBEGA1UECxMKZW1TaWduIFBLSTElMCMGA1UEChMcZU11ZGhy
YSBUZWNobm9sb2dpZXMgTGltaXRlZDEgMB4GA1UEAxMXZW1TaWduIEVDQyBSb290
IENBIC0gRzMwHhcNMjQwNzExMTU0MzM2WhcNMzkwNzA5MTgzMDAwWjBWMQswCQYD
VQQGEwJJTjElMCMGA1UECgwcZU11ZGhyYSBUZWNobm9sb2dpZXMgTGltaXRlZDEg
MB4GA1UEAwwXZW1TaWduIFJvb3QgVExTIENBIC0gRzMwdjAQBgcqhkjOPQIBBgUr
gQQAIgNiAARfD9w/q1cWxL6D9bMGH6CQ4u+gAV6GN9s+PwQx9Bl4hC+0iU8QOk8m
WO/RwXQy10DCSxT8vSIJClHKp+Ao4qJKdYeV0SwZ7LMQziyLfxIUOY6Grja/sQFw
LKYf8EdDXAyjggF1MIIBcTAfBgNVHSMEGDAWgBR8XQKEE9TMipuBzhccLikenEhj
QjAdBgNVHQ4EFgQU4TqrIXzx401UrtRyt/2LWUIgkFQwHQYDVR0lBBYwFAYIKwYB
BQUHAwEGCCsGAQUFBwMCMD0GA1UdIAQ2MDQwMgYEVR0gADAqMCgGCCsGAQUFBwIB
FhxodHRwOi8vcmVwb3NpdG9yeS5lbXNpZ24uY29tMHgGCCsGAQUFBwEBBGwwajAi
BggrBgEFBQcwAYYWaHR0cDovL29jc3AuZW1zaWduLmNvbTBEBggrBgEFBQcwAoY4
aHR0cDovL3JlcG9zaXRvcnkuZW1zaWduLmNvbS9jZXJ0cy9lbVNpZ25FQ0NSb290
Q0FHMy5jcnQwMwYDVR0fBCwwKjAooCagJIYiaHR0cDovL2NybC5lbXNpZ24uY29t
P1Jvb3RDQUczLmNybDAOBgNVHQ8BAf8EBAMCAQYwEgYDVR0TAQH/BAgwBgEB/wIB
ATAKBggqhkjOPQQDAwNpADBmAjEAt9CIpRGp46QHQYDUBnLKw+vvK0/h9vmilEAg
TroUl/anH4x2rXHqiCtBTBfgKDegAjEA8WpG1LTZdAIL0FSYqxXeREaeTSxEofM0
1jJH3jGvCXC79LU9UPJ2o3TU+UIj2gMV
-----END CERTIFICATE-----
EOF
# dv-g3b-aaa-emudhra.we-have-a-problem.com: leaf -> EM DV TLS CA - G3B (signed
# by AAA Certificate Services).
cat >"$tmp/aaa-emudhra.pem" <<'EOF'
-----BEGIN CERTIFICATE-----
MIICQDCCAcagAwIBAgIUU3mgjls/8ZJ8upmukdlSQR1IF/UwCgYIKoZIzj0EAwIw
PzELMAkGA1UEBhMCTkwxEzARBgNVBAoMCmVNdWRocmEgQlYxGzAZBgNVBAMMEkVN
IERWIFRMUyBDQSAtIEczQjAeFw0yNjEwMjAwMjIyNDFaFw0yNzA1MDMwMjI3NDFa
MDMxMTAvBgNVBAMMKGR2LWczYi1hYWEtZW11ZGhyYS53ZS1oYXZlLWEtcHJvYmxl
bS5jb20wWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAATK3sfcOxxtM2Vdq9Qbw/GR
NsHlJcnqINEInXO3fkWhw/FYT80hkqGk0jSN/EKib3YZJsr5GyfkYcpEiA2rwMgm
o4GrMIGoMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgeAMBMGA1UdJQQMMAoG
CCsGAQUFBwMBMDMGA1UdEQQsMCqCKGR2LWczYi1hYWEtZW11ZGhyYS53ZS1oYXZl
LWEtcHJvYmxlbS5jb20wHQYDVR0OBBYEFDggfJFz1eeToKmzFBSYkMCvpAu0MB8G
A1UdIwQYMBaAFOIeL+ccGTkOVUtiwSAkVzUkiiUUMAoGCCqGSM49BAMCA2gAMGUC
MDqDDOt7K313QIH0rhn/ZKnzFu8+/9W8OlTuFtDfVCTRDfRmqdyfsCfLsEk7xqHq
uQIxAJqyLERABmU98U46OCuT5bp5h0ytuSEIGM7orTpKci5S1u1B/AOmwv162U7R
TcVPHA==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIDzjCCAragAwIBAgIRAKAk/fS6Zlx4XOSgIEg4WLcwDQYJKoZIhvcNAQELBQAw
ezELMAkGA1UEBhMCR0IxGzAZBgNVBAgMEkdyZWF0ZXIgTWFuY2hlc3RlcjEQMA4G
A1UEBwwHU2FsZm9yZDEaMBgGA1UECgwRQ29tb2RvIENBIExpbWl0ZWQxITAfBgNV
BAMMGEFBQSBDZXJ0aWZpY2F0ZSBTZXJ2aWNlczAeFw0yNTA1MTQwMDAwMDBaFw0y
ODEyMzEyMzU5NTlaMD8xCzAJBgNVBAYTAk5MMRMwEQYDVQQKDAplTXVkaHJhIEJW
MRswGQYDVQQDDBJFTSBEViBUTFMgQ0EgLSBHM0IwdjAQBgcqhkjOPQIBBgUrgQQA
IgNiAATnntnASpdzxHNeUREdqFrCvV5kqzdLgOEsvCA47B5PDjuBL8mFnh5nr7BO
L4vJf3SFumWqkR7opORbYtXMA9Vr5nizw4y03JN8okYOI4RTyJKTXSnUT5HhVga2
PM7atGijggE1MIIBMTAfBgNVHSMEGDAWgBSgEQojPpbxB+zirynvgqV/0DCktDAd
BgNVHQ4EFgQU4h4v5xwZOQ5VS2LBICRXNSSKJRQwDgYDVR0PAQH/BAQDAgEGMBIG
A1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMC
MDEGA1UdIAQqMCgwCAYGZ4EMAQIBMA0GCysGAQQBg44hAQABMA0GCysGAQQBg44h
AQJkMEMGA1UdHwQ8MDowOKA2oDSGMmh0dHA6Ly9jcmwuY29tb2RvY2EuY29tL0FB
QUNlcnRpZmljYXRlU2VydmljZXMuY3JsMDQGCCsGAQUFBwEBBCgwJjAkBggrBgEF
BQcwAYYYaHR0cDovL29jc3AuY29tb2RvY2EuY29tMA0GCSqGSIb3DQEBCwUAA4IB
AQBzQ1IoqpdotM6gFrR4Q3YrKifOLeaj5XXunldpVE/vZn2TxC2y0s8oYiLR5ICE
wqVXSB7N6BM1Mp+Nif7Jbhb2IswAfSR4Iik8+B0iRmi7lYoNlycCgHjmsjOmZaxM
QkPmjXAJbG5f4V41OsvJZmS7lI3r0IuvEDVX3GAZ13on6IImSs9ybNpHQ7NCqkMQ
7wH4uAO1dtpFycgJSdHaCOhHjrox2BTYGLdpUrYwY3EOAjlZ8UGgsxs7mG+xquMl
bN1LFURwfnkVMP3T/DBIZ/ys2+lNFyxK3FlArrV4/000uTnVB1lO8oGORYdXoT5V
GUyRCEIDhF5TBUuKF4E3kf1h
-----END CERTIFICATE-----
EOF

# Passes only if openssl can't build the chain to a trusted root (error 20,
# "unable to get local issuer certificate", on both OpenSSL 1.1.1 and 3.x).
chain_untrusted() {
  local pem=$1 out line n=0
  # The default CApath/store openssl also loads only add trust anchors, so they
  # can only make this stricter. 1793491200 is 2026-11-01T00:00:00Z. verify
  # checks the file's first certificate; the rest serve as untrusted
  # intermediates.
  if out=$(openssl verify -attime 1793491200 -CAfile "$bundle" -untrusted "$pem" "$pem" 2>&1); then
    printf 'the image trusts the compromised chain:\n%s\n' "$out" >&2
    return 1
  fi
  # Needs at least one "error N at" line, all with N = 20, and no "OK".
  while IFS= read -r line; do
    if [[ "$line" == *": OK" ]]; then n=0; break; fi
    [[ "$line" =~ ^error\ ([0-9]+)\ at\ [0-9]+\ depth\ lookup ]] || continue
    if [[ "${BASH_REMATCH[1]}" != 20 ]]; then n=0; break; fi
    n=$((n + 1))
  done <<<"$out"
  if ((n == 0)); then
    printf 'unexpected openssl verify result:\n%s\n' "$out" >&2
    return 1
  fi
}

echo "Checking Ubuntu $family ($(uname -m))"
fail=0
check() {
  local name=$1
  shift
  # stderr is kept: it explains a failure.
  if "$@" >/dev/null; then echo "ok      $name"; else echo "FAILED  $name"; fail=1; fi
}
for t in $tools; do
  if command -v "$t" >/dev/null; then echo "ok      $t"; else echo "MISSING $t"; fail=1; fi
done
for r in "${runs[@]}"; do
  check "${r% 2>&1}" bash -c "$r"
done
check 'en_US.UTF-8 locale' bash -c "locale -a | grep -qix 'en_US.utf8'"
if ((setup)); then
  check 'buildbuddy user' id buildbuddy
  check 'buildbuddy in sudo, docker' bash -c "id -nG buildbuddy | tr ' ' '\\n' | grep -qx sudo && id -nG buildbuddy | tr ' ' '\\n' | grep -qx docker"
  check 'iptables-legacy' bash -c "update-alternatives --query iptables | grep -qx 'Value: /usr/sbin/iptables-legacy'"
  check 'ip6tables-legacy' bash -c "update-alternatives --query ip6tables | grep -qx 'Value: /usr/sbin/ip6tables-legacy'"
fi
check 'no distrusted CA roots' distrusted_roots_absent
check 'eMudhra chain via emSign ECC Root CA - G3 untrusted' chain_untrusted "$tmp/emudhra.pem"
check 'eMudhra chain via AAA Certificate Services untrusted' chain_untrusted "$tmp/aaa-emudhra.pem"
check https curl -fsS --connect-timeout 10 --max-time 30 -o /dev/null https://www.google.com
check 'git ls-remote' timeout 60 git ls-remote https://github.com/bazelbuild/bazel HEAD
exit "$fail"
