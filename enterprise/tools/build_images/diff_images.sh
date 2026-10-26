#!/usr/bin/env bash
# Compares the contents of two Debian/Ubuntu-based images for one platform:
# installed package versions, plus every file's type, mode, owner, symlink
# target and SHA-256. File changes are attributed to the dpkg package that owns
# them; changes not explained by a package being added, removed or upgraded are
# listed in full.
#
# Usage:
#   ./enterprise/tools/build_images/diff_images.sh OLD_IMAGE NEW_IMAGE PLATFORM OUT_DIR
#
# Writes {old,new}/{packages,owners,files}.tsv and packages.diff,
# files-explained.txt and files-unexplained.txt to OUT_DIR. The container CLI
# defaults to docker, falling back to Apple's `container`; set CONTAINER_CLI to
# override.
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: $0 OLD_IMAGE NEW_IMAGE PLATFORM OUT_DIR" >&2
  exit 2
fi
OLD_IMAGE="$1"
NEW_IMAGE="$2"
PLATFORM="$3"
OUT_DIR="$4"

if [[ -z "${CONTAINER_CLI:-}" ]]; then
  if command -v docker >/dev/null; then
    CONTAINER_CLI=docker
  else
    CONTAINER_CLI=container
  fi
fi

# Runs inside the image and writes its inventory to /out.
read -r -d '' INVENTORY <<'EOF' || true
set -euo pipefail
export LC_ALL=C
# usr-merge: dpkg may record /bin/x while the file lives at /usr/bin/x.
norm() { sed -E 's#^/(bin|sbin|lib|lib32|lib64|libx32)/#/usr/\1/#'; }

dpkg-query -W -f='${Package}:${Architecture}\t${Version}\n' | sort > /out/packages.tsv

for list in /var/lib/dpkg/info/*.list; do
  pkg=$(basename "$list" .list)
  [[ "$pkg" == *:* ]] || pkg="$pkg:$(dpkg-query -W -f='${Architecture}' "$pkg" 2>/dev/null || echo unknown)"
  norm < "$list" | sed "s#^#$pkg\t#"
done | sort -t$'\t' -k2,2 -u > /out/owners.tsv

# Paths that are runtime mounts, caches, logs, or package-manager state.
prune=(/proc /sys /dev /run /tmp /var/tmp /var/cache /var/log /var/lib/apt/lists /var/lib/dpkg /out)
args=()
for p in "${prune[@]}"; do args+=(-path "$p" -o); done
unset 'args[${#args[@]}-1]'
find / -xdev \( "${args[@]}" \) -prune -o \
  \( -path /etc/hostname -o -path /etc/hosts -o -path /etc/resolv.conf \) -prune -o \
  ! -type d -printf '%p\t%y\t%m\t%U:%G\t%l\n' | sort -t$'\t' -k1,1 > /tmp/meta.tsv
find / -xdev \( "${args[@]}" \) -prune -o \
  \( -path /etc/hostname -o -path /etc/hosts -o -path /etc/resolv.conf \) -prune -o \
  -type f -print0 | xargs -0 -r sha256sum | sed -E 's#^([0-9a-f]+)  (.*)$#\2\t\1#' | sort -t$'\t' -k1,1 > /tmp/sha.tsv
join -t$'\t' -a1 -e - -o 1.1,1.2,1.3,1.4,1.5,2.2 /tmp/meta.tsv /tmp/sha.tsv > /out/files.tsv
EOF

inventory() {
  local image="$1" dir="$2"
  mkdir -p "$dir"
  echo "Collecting inventory of $image ($PLATFORM)..." >&2
  "$CONTAINER_CLI" run --rm --platform "$PLATFORM" -v "$(cd "$dir" && pwd):/out" "$image" bash -c "$INVENTORY"
}

mkdir -p "$OUT_DIR"
inventory "$OLD_IMAGE" "$OUT_DIR/old"
inventory "$NEW_IMAGE" "$OUT_DIR/new"

export LC_ALL=C

# Package changes: "+ pkg ver", "- pkg ver", "~ pkg old -> new".
join -t$'\t' -a1 -a2 -e - -o 0,1.2,2.2 "$OUT_DIR/old/packages.tsv" "$OUT_DIR/new/packages.tsv" |
  awk -F'\t' '
    $2 == "-" { print "+ " $1 " " $3; next }
    $3 == "-" { print "- " $1 " " $2; next }
    $2 != $3  { print "~ " $1 " " $2 " -> " $3 }
  ' > "$OUT_DIR/packages.diff"

# File changes, attributed to their owning package (from either image). A
# change is "explained" if its owner was added, removed or changed version.
awk -F'\t' -v OFS='\t' '
  FILENAME ~ /packages.diff$/ { split($0, a, " "); changed[a[2]] = 1; next }
  FILENAME ~ /owners.tsv$/    { owner[$2] = $1; next }
  FILENAME ~ /old\/files.tsv$/ { old[$1] = $2 OFS $3 OFS $4 OFS $5 OFS $6; next }
  { new[$1] = $2 OFS $3 OFS $4 OFS $5 OFS $6 }
  END {
    for (p in old) if (!(p in new)) report("-", p)
    for (p in new) if (!(p in old)) report("+", p); else if (old[p] != new[p]) report("~", p)
  }
  function report(kind, p,   o) {
    o = (p in owner) ? owner[p] : "(no package)"
    if (o in changed) print "explained", kind, o, p
    else print "unexplained", kind, o, p, (p in old ? old[p] : ""), (p in new ? new[p] : "")
  }
' "$OUT_DIR/packages.diff" "$OUT_DIR/old/owners.tsv" "$OUT_DIR/new/owners.tsv" \
  "$OUT_DIR/old/files.tsv" "$OUT_DIR/new/files.tsv" > "$OUT_DIR/files.tmp"

awk -F'\t' '$1 == "explained" { print $3 }' "$OUT_DIR/files.tmp" | sort | uniq -c | sort -k2 > "$OUT_DIR/files-explained.txt"
{
  echo -e "# change\towner\tpath\told (type mode uid:gid target sha256)\tnew"
  awk -F'\t' -v OFS='\t' '$1 == "unexplained" { $1 = ""; sub(/^\t/, ""); print }' "$OUT_DIR/files.tmp" | sort -t$'\t' -k3,3
} > "$OUT_DIR/files-unexplained.txt"
rm "$OUT_DIR/files.tmp"

echo
echo "=== Packages ($PLATFORM): $(grep -c '^+' "$OUT_DIR/packages.diff" || true) added, $(grep -c '^-' "$OUT_DIR/packages.diff" || true) removed, $(grep -c '^~' "$OUT_DIR/packages.diff" || true) changed"
cat "$OUT_DIR/packages.diff"
echo
echo "=== Files changed by those packages (count per package): $OUT_DIR/files-explained.txt"
echo "=== Files changed outside of package changes: $(($(wc -l < "$OUT_DIR/files-unexplained.txt") - 1))"
cut -f1-3 "$OUT_DIR/files-unexplained.txt" | tail -n +2
