FROM fedora:latest

RUN dnf -y install podman
# RUN dnf update --refresh --enablerepo=updates-testing podman
