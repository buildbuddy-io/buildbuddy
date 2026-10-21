#!/usr/bin/env bash
# Checks a CA patch image against the image it was built from. It never
# builds, so it validates exactly the image that will be (or was) pushed.
# Exits nonzero on any failure.
#   ./validate.sh BASE_IMAGE PATCHED_IMAGE [CERTS]
# Both refs must be single-platform images (for a multi-arch index, pass the
# per-platform child digests). CERTS defaults to both roots; pass
# "mozilla/Comodo_AAA_Services_root.crt" for Ubuntu 16.04.
set -uo pipefail

BASE="$1"
PATCHED="$2"
CERTS="${3:-mozilla/emSign_ECC_Root_CA_-_G3.crt mozilla/Comodo_AAA_Services_root.crt}"
BAD_HOSTS=(dv-g3b-emudhra.we-have-a-problem.com dv-g3b-aaa-emudhra.we-have-a-problem.com)
DIR="$(cd "$(dirname "$0")" && pwd)"

rc=0
fail() { echo "FAIL: $*"; rc=1; }
section() { echo; echo "----- $* -----"; }
ensure() { docker image inspect "$1" >/dev/null 2>&1 || docker pull -q "$1" >/dev/null || { echo "FAIL: cannot pull $1"; exit 1; }; }
platform() { docker image inspect -f '{{.Os}}/{{.Architecture}}' "$1"; }
# in_image IMAGE CMD: runs CMD as root with sh; returns its exit code.
in_image() { docker run --rm --platform "$(platform "$1")" -u 0 --entrypoint /bin/sh "$1" -c "$2"; }

ensure "$BASE"
ensure "$PATCHED"

section "images"
for img in "$BASE" "$PATCHED"; do
  echo "$img: $(docker image inspect -f '{{.Id}} {{.Os}}/{{.Architecture}}' "$img")"
done

section "layers"
base_layers=$(docker image inspect -f '{{range .RootFS.Layers}}{{println .}}{{end}}' "$BASE" | grep .)
new_layers=$(docker image inspect -f '{{range .RootFS.Layers}}{{println .}}{{end}}' "$PATCHED" | grep .)
nbase=$(wc -l <<<"$base_layers")
added=$(($(wc -l <<<"$new_layers") - nbase))
[[ "$(head -n "$nbase" <<<"$new_layers")" == "$base_layers" ]] || fail "base layers changed"
[[ "$added" == 1 ]] || fail "expected 1 added layer, got $added"
echo "base layers: $nbase, added: $added"
docker history --no-trunc --format '{{.Size}}  {{.CreatedBy}}' "$PATCHED" | head -1 | cut -c1-160

section "config"
cfg() { docker image inspect -f '{{json .Config}}' "$1" | sed -E 's/"Image":"[^"]*",//'; }
diff <(cfg "$BASE") <(cfg "$PATCHED") || fail "runtime config differs"

section "package changes"
q='dpkg-query -W -f "\${Package}:\${Architecture} \${Version}\n" | sort'
pkgdiff=$(diff <(in_image "$BASE" "$q") <(in_image "$PATCHED" "$q") | grep '^[<>]')
echo "${pkgdiff:-(none)}"
other=$(grep -v '^[<>] ca-certificates:all ' <<<"$pkgdiff" | grep .)
[[ -z "$other" ]] || fail "packages other than ca-certificates changed"

section "trust store"
for img in "$BASE" "$PATCHED"; do
  echo "[$img] $(in_image "$img" 'dpkg-query -W -f "\${Version}" ca-certificates')"
  in_image "$img" 'grep -E "emSign_ECC_Root_CA_-_G3|Comodo_AAA_Services_root" /etc/ca-certificates.conf'
done
docker run --rm --platform "$(platform "$PATCHED")" -u 0 --entrypoint /bin/sh \
  -v "$DIR/check_roots_removed.sh:/ca_patch_check.sh:ro" "$PATCHED" /ca_patch_check.sh $CERTS ||
  fail "check_roots_removed.sh failed on the patched image"

section "TLS"
for h in "${BAD_HOSTS[@]}"; do
  in_image "$BASE" "curl -sS -o /dev/null --max-time 20 https://$h" >/dev/null 2>&1
  echo "base:    $h curl exit $?"
  out=$(in_image "$PATCHED" "curl -sS -o /dev/null --max-time 20 https://$h" 2>&1)
  code=$?
  echo "patched: $h curl exit $code: $out"
  [[ "$code" == 60 ]] || fail "$h: expected certificate verification error (curl exit 60), got exit $code"
done
code=$(in_image "$PATCHED" "curl -sS -o /dev/null -w '%{http_code}' --max-time 20 https://www.google.com")
echo "https://www.google.com: $code"
[[ "$code" == 200 ]] || fail "https://www.google.com: got $code"
if in_image "$PATCHED" 'git ls-remote https://github.com/bazelbuild/bazel HEAD' >/dev/null; then
  echo "git ls-remote github.com: ok"
else
  fail "git ls-remote https://github.com/bazelbuild/bazel HEAD"
fi
if in_image "$PATCHED" 'command -v python3' >/dev/null; then
  if in_image "$PATCHED" "python3 -c 'import urllib.request; urllib.request.urlopen(\"https://pypi.org\", timeout=20)'"; then
    echo "python3 urlopen https://pypi.org: ok"
  else
    fail "python3 urlopen https://pypi.org"
  fi
else
  echo "python3 urlopen https://pypi.org: skipped (no python3)"
fi

section "java"
in_image "$PATCHED" '
  command -v java >/dev/null || { echo "no java"; exit 0; }
  jh=$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")
  for f in "$jh/lib/security/cacerts" "$jh/jre/lib/security/cacerts"; do
    [ -e "$f" ] && { echo "java uses $f -> $(readlink -f "$f")"; exit 0; }
  done
  echo "java cacerts not found under $jh"'

echo
if [[ "$rc" == 0 ]]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; fi
exit "$rc"
