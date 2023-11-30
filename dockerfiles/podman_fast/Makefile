all: .podman-fedora

.podman-%: %.Dockerfile
	podman build -f $^ -t podman-$*
	touch $@
