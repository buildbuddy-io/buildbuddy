#!/usr/bin/env bash
# Builds a CA patch image locally and validates it against its base.
# Pushes nothing. Usage (from this directory):
#   ./validate.sh BASE_IMAGE_BY_DIGEST DOCKERFILE LOCAL_TAG
# e.g.
#   ./validate.sh gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db... Dockerfile ca-patch/rbe-ubuntu24-04
set -uo pipefail

BASE="$1"
DOCKERFILE="$2"
TAG="$3"
BAD_HOSTS=(dv-g3b-emudhra.we-have-a-problem.com dv-g3b-aaa-emudhra.we-have-a-problem.com)

section() { echo; echo "----- $* -----"; }
in_image() { docker run --rm -u 0 --entrypoint /bin/bash "$1" -c "$2" 2>&1; }

docker pull -q "$BASE" >/dev/null || { echo "FAIL: pull $BASE"; exit 1; }
docker build -q --build-arg BASE="$BASE" -f "$DOCKERFILE" -t "$TAG" . >/dev/null || { echo "FAIL: build"; exit 1; }

section "image IDs"
echo "base:    $(docker inspect -f '{{.Id}} user=[{{.Config.User}}] {{.Os}}/{{.Architecture}}' "$BASE")"
echo "patched: $(docker inspect -f '{{.Id}} user=[{{.Config.User}}] {{.Os}}/{{.Architecture}}' "$TAG")"

section "package changes (dpkg-query -W, base -> patched)"
q='dpkg-query -W -f "\${Package}:\${Architecture} \${Version}\n" | sort'
diff <(in_image "$BASE" "$q") <(in_image "$TAG" "$q") && echo "(none)"

section "layers"
base_layers=$(docker inspect -f '{{range .RootFS.Layers}}{{println .}}{{end}}' "$BASE" | grep .)
new_layers=$(docker inspect -f '{{range .RootFS.Layers}}{{println .}}{{end}}' "$TAG" | grep .)
if [[ "$(head -n "$(wc -l <<<"$base_layers")" <<<"$new_layers")" == "$base_layers" ]]; then
  echo "base layers unchanged: yes ($(wc -l <<<"$base_layers") layers)"
else
  echo "base layers unchanged: NO"
fi
echo "added layers: $(($(wc -l <<<"$new_layers") - $(wc -l <<<"$base_layers")))"
docker history --no-trunc --format '{{.Size}}  {{.CreatedBy}}' "$TAG" | head -1 | cut -c1-200
echo "runtime config (ignoring the legacy .Config.Image field) identical:"
cfg() { docker inspect -f '{{json .Config}}' "$1" | sed -E 's/"Image":"[^"]*",//'; }
diff <(cfg "$BASE") <(cfg "$TAG") && echo "  yes"

section "trust store"
for img in "$BASE" "$TAG"; do
  echo "[$img]"
  in_image "$img" 'dpkg-query -W ca-certificates; grep -E "emSign_ECC_Root_CA_-_G3|Comodo_AAA_Services_root" /etc/ca-certificates.conf; ls /etc/ssl/certs | grep -iE "emSign_ECC_Root_CA_-_G3|Comodo_AAA" || echo "(no /etc/ssl/certs links for the two roots)"'
done

section "TLS checks"
for img in "$BASE" "$TAG"; do
  echo "[$img]"
  for h in "${BAD_HOSTS[@]}"; do
    echo -n "curl $h: "; in_image "$img" "curl -sS -o /dev/null -w '%{http_code}' --max-time 20 https://$h; echo \" exit=\$?\""
  done
  if [[ "$img" == "$TAG" ]]; then
    for h in "${BAD_HOSTS[@]}"; do
      echo -n "python3 $h: "; in_image "$img" "python3 -c 'import urllib.request;urllib.request.urlopen(\"https://$h\", timeout=20)' >/dev/null 2>&1 && echo ACCEPTED || echo rejected"
    done
    echo -n "curl www.google.com: "; in_image "$img" "curl -sS -o /dev/null -w '%{http_code}' --max-time 20 https://www.google.com; echo \" exit=\$?\""
    echo -n "git ls-remote bazel HEAD: "; in_image "$img" 'git ls-remote https://github.com/bazelbuild/bazel HEAD >/dev/null && echo ok || echo FAIL'
    echo -n "python3 urlopen pypi.org: "; in_image "$img" "python3 -c 'import urllib.request;urllib.request.urlopen(\"https://pypi.org\", timeout=20)' && echo ok || echo FAIL"
  fi
done

section "java cacerts"
for img in "$BASE" "$TAG"; do
  echo "[$img]"
  in_image "$img" '
    command -v java >/dev/null || { echo "no java"; exit 0; }
    jh=$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")
    ks=""; for f in "$jh/lib/security/cacerts" "$jh/jre/lib/security/cacerts"; do [ -e "$f" ] && ks=$f && break; done
    echo "cacerts: $ks -> $(readlink -f "$ks")"
    keytool -list -keystore "$ks" -storepass changeit 2>/dev/null | grep -iE "emsign.*ecc.*g3|aaa" || echo "(no emSign ECC G3 / AAA entries)"'
done

section "smoke"
in_image "$TAG" '
  cd /tmp && printf "#include <stdio.h>\nint main(){puts(\"hello\");}\n" > h.c && cp h.c h.cc
  for cc in gcc g++ clang; do
    command -v $cc >/dev/null || { echo "$cc: absent"; continue; }
    src=h.c; [ $cc = g++ ] && src=h.cc
    $cc $src -o h && ./h >/dev/null && echo "$cc: ok" || echo "$cc: FAIL"
  done
  command -v java >/dev/null && { java -version 2>&1 | head -1; } || echo "java: absent"
  command -v python3 >/dev/null && python3 --version || echo "python3: absent"'

docker rmi -f "$TAG" "$BASE" >/dev/null
docker image prune -f >/dev/null
docker builder prune -af >/dev/null
