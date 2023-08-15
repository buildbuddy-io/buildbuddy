#!/usr/bin/env bash
set -e

export USER=root
export HOME=/root
export GOROOT=/usr/local/go
export GO_REV=1f9d80e331


#cat /proc/meminfo | grep Huge
#cat /proc/filesystems | grep huge
#cat /proc/mounts | grep huge
#cat /sys/kernel/mm/transparent_hugepage/hpage_pmd_size
#apt-cache rdepends --installed dkms
#ls /asdfkjsdf/sdfksfd

#rm -rf /usr/local/go
#curl -fsSL https://storage.googleapis.com/buildbuddy-tools/binaries/golang/go-${GO_REV}.tar | tar --directory /usr/local -xzf -

export PATH="$PATH:/usr/local/go/bin"
export PATH="$PATH:$(go env GOPATH)/bin"
 
# go env GOROOT=/usr/local/go
which go;
go version;
uname -a;
#echo never > /sys/kernel/mm/transparent_hugepage/enabled

MODULES=($GO_MODULES)

# Generate a Makefile so we can use Make to download in parallel (make -j<n>)
[[ -e Makefile ]] && rm Makefile
for i in $(seq 1 "${#MODULES[@]}"); do
  idx=$((i - 1))
  {
    printf '%d:\n' "$i"
    printf '\tgo mod download %s\n\n' "${MODULES[$idx]}"
  } >>Makefile
done
{
  printf 'all: '
  printf '%s ' $(seq 1 "${#MODULES[@]}")
  printf '\n\n'
} >>Makefile

go clean -modcache
make -j$((CPU_COUNT * 2)) all >&2
