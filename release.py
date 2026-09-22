#!/usr/bin/env python3
import argparse
import os
import platform
import re
import requests
import subprocess
import sys

"""
release.py - A simple script to create a release.

This script will do the following:

  1) Check that your working repository is clean.
  2) If --tag is set, create that version tag pointing at HEAD. The version
     comes from the release branch's VERSION file in buildbuddy-internal.
  3) Pushes the tag to GitHub.
     This kicks off some workflows which will build the release artifacts.
  4) Builds and tags new Docker images locally, and pushes them to the registry.
     Also updates the ":latest" tag for each image.
"""

def die(message):
    print(message)
    sys.exit(1)

def run_or_die(cmd, capture_stdout=False):
    print("(debug) running cmd: %s" % cmd)
    stdout = sys.stdout
    if capture_stdout:
        stdout = subprocess.PIPE
    p = subprocess.run(cmd, shell=True, stdout=stdout, stderr=sys.stderr, encoding='utf-8')
    if p.returncode != 0:
        die("Command failed with code %d" % (p.returncode))
    return p

def nonempty_lines(text):
    lines = text.split('\n')
    lines = [line for line in lines if line]
    return lines

def workspace_is_clean():
    print('Checking if workspace is clean.')
    p = subprocess.Popen('git status --untracked-files=no --porcelain',
                         shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    out = [l.decode() for l in p.stdout.readlines()]
    print('git status output:\n%s' % "\n".join(out))
    return len(out) == 0

VERSION_TAG_RE = re.compile(r'^v\d+\.\d+\.\d+$')

def yes_or_no(question):
    while "the answer is invalid":
        reply = input(question+" (y/n): ").lower().strip()
        if reply[:1] == "y":
            return True
        if reply[:1] == "n":
            return False

def remote_tag_exists(version_tag):
    p = run_or_die(f'git ls-remote --tags origin refs/tags/{version_tag}', capture_stdout=True)
    return p.stdout.strip() != ''

IMAGE_INDEX_MEDIA_TYPES = [
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
]

IMAGE_MANIFEST_MEDIA_TYPES = IMAGE_INDEX_MEDIA_TYPES + [
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
]

LINUX_IMAGE_PLATFORMS = [
    ("amd64", "//platforms:linux_x86_64"),
    ("arm64", "//platforms:linux_arm64"),
]

def get_image(project, tag):
    query_url = f"https://gcr.io/v2/{project}/manifests/{tag}"
    r = requests.get(query_url, headers={"Accept": ", ".join(IMAGE_MANIFEST_MEDIA_TYPES)})
    if r.status_code == 404:
        return None
    if not r.ok:
        die(f"Could not fetch image gcr.io/{project}:{tag}: HTTP {r.status_code}")
    digest = r.headers.get("Docker-Content-Digest")
    if digest is None:
        die(f"Registry did not return a digest for image gcr.io/{project}:{tag}.")
    media_type = r.headers.get("Content-Type", "").split(";")[0].strip().lower()
    if media_type not in IMAGE_MANIFEST_MEDIA_TYPES:
        die(f"Registry returned an unsupported media type for image gcr.io/{project}:{tag}: {media_type!r}")
    return {"digest": digest, "media_type": media_type}

def create_and_push_tag(version_tag):
    run_or_die(f'git tag -a {version_tag} -m "Release {version_tag} (release.py)"')
    run_or_die(f'git push origin {version_tag}')

def push_image_for_project(project, version_tag, bazel_target, skip_update_latest_tag, platform_target=None):
    version_image = get_image(project, version_tag)
    if version_image is None:
        build_image_with_bazel(bazel_target, platform_target)
        tag_and_push_image_with_docker(bazel_target, project, version_tag)
    else:
        print(f'Image gcr.io/{project}:{version_tag} already exists, skipping bazel build.')

    update_latest_tag(project, version_tag, version_image, skip_update_latest_tag)

def push_multi_platform_image_for_project(project, version_tag, bazel_target, skip_update_latest_tag):
    version_image = get_image(project, version_tag)
    if version_image is not None and version_image["media_type"] not in IMAGE_INDEX_MEDIA_TYPES:
        die(f"Image gcr.io/{project}:{version_tag} already exists but is not multi-platform. Use a new version tag.")
    if version_image is None:
        architecture_images = []
        for architecture, platform_target in LINUX_IMAGE_PLATFORMS:
            architecture_tag = f"{version_tag}-{architecture}"
            push_image_for_project(
                project, architecture_tag, bazel_target,
                skip_update_latest_tag=True, platform_target=platform_target,
            )
            architecture_images.append(f"gcr.io/{project}:{architecture_tag}")
        create_and_push_multi_platform_manifest(project, version_tag, architecture_images)
    else:
        print(f'Image gcr.io/{project}:{version_tag} already exists, skipping bazel build.')

    update_latest_tag(project, version_tag, version_image, skip_update_latest_tag)

def update_latest_tag(project, version_tag, version_image, skip_update_latest_tag):
    if skip_update_latest_tag:
        return

    version_image = version_image or get_image(project, version_tag)
    if version_image is None:
        die(f"Could not fetch image with tag {version_tag} from project {project}.")

    latest_image = get_image(project, "latest")
    if latest_image is None:
        print(f"No 'latest' tag found for {project}; tagging current version as latest.")
    elif (
        latest_image["media_type"] in IMAGE_INDEX_MEDIA_TYPES
        and version_image["media_type"] not in IMAGE_INDEX_MEDIA_TYPES
    ):
        die(f"Refusing to replace multi-platform gcr.io/{project}:latest with single-platform {version_tag}. "
            "Publish a multi-platform image or use --skip_latest_tag.")
    elif version_image["digest"] == latest_image["digest"]:
        return

    add_tag_cmd = f"echo 'yes' | gcloud container images add-tag gcr.io/{project}:{version_tag} gcr.io/{project}:latest"
    run_or_die(add_tag_cmd)

def build_image_with_bazel(bazel_target, platform_target=None):
    print(f"Building docker image target {bazel_target}")
    # Note: we are not using container_push targets here, because it has a bug
    # where it uses "application/vnd.oci.image.layer.v1.tar" mediaType for some
    # image layers on arm64, which podman and containerd cannot handle.
    # https://github.com/buildbuddy-io/buildbuddy-internal/issues/3316
    platform_flag = f" --platforms={platform_target}" if platform_target else ""
    run_or_die(f'bazel run -c opt --stamp --define=release=true{platform_flag} {bazel_target}')

def tag_and_push_image_with_docker(bazel_target, project, version_tag):
    # rules_docker uses a convention where "//PACKAGE:LABEL" gets locally tagged
    # with "bazel/PACKAGE:LABEL".
    local_image_ref = bazel_target.replace('//', 'bazel/')
    remote_image_ref = f'gcr.io/{project}:{version_tag}'
    print(f'Tagging and pushing {remote_image_ref}')
    run_or_die(f'docker tag {local_image_ref} {remote_image_ref}')
    run_or_die(f'docker push {remote_image_ref}')

def create_and_push_multi_platform_manifest(project, version_tag, architecture_images):
    remote_image_ref = f'gcr.io/{project}:{version_tag}'
    print(f'Creating and pushing multi-platform manifest {remote_image_ref}')
    # A failed push leaves the local manifest behind; allow the release to retry.
    run_or_die(f'docker manifest create --amend {remote_image_ref} {" ".join(architecture_images)}')
    run_or_die(f'docker manifest push --purge {remote_image_ref}')

def update_docker_images(images, version_tag, skip_update_latest_tag, arch_specific_executor_tag, arch_specific_proxy_tag):
    clean_cmd = 'bazel clean --expunge'
    run_or_die(clean_cmd)

    # OSS app
    if 'buildbuddy-app-onprem' in images:
        push_multi_platform_image_for_project("flame-public/buildbuddy-app-onprem", version_tag, '//server/cmd/buildbuddy:buildbuddy_image', skip_update_latest_tag)
    # Enterprise app
    if 'buildbuddy-app-enterprise' in images:
        push_image_for_project("flame-public/buildbuddy-app-enterprise", 'enterprise-' + version_tag, '//enterprise/server/cmd/server:buildbuddy_image', skip_update_latest_tag)
    # Enterprise executor
    if 'buildbuddy-executor-enterprise' in images:
        executor_tag = 'enterprise-' + version_tag
        if arch_specific_executor_tag:
            executor_tag += '-' + get_cpu_architecture()
        # Skip "latest" tag for arch-specific images, since the latest tag
        # should only apply to the multiarch one.
        skip_latest_tag = skip_update_latest_tag or arch_specific_executor_tag
        push_image_for_project("flame-public/buildbuddy-executor-enterprise", executor_tag, '//enterprise/server/cmd/executor:executor_image', skip_latest_tag)
    # Enterprise cache proxy
    if 'buildbuddy-proxy-enterprise' in images:
        proxy_tag = 'enterprise-' + version_tag
        if arch_specific_proxy_tag:
            proxy_tag += '-' + get_cpu_architecture()
        # Skip "latest" tag for arch-specific images, since the latest tag
        # should only apply to the multiarch one.
        skip_latest_tag = skip_update_latest_tag or arch_specific_proxy_tag
        push_image_for_project("flame-public/buildbuddy-proxy-enterprise", proxy_tag, '//enterprise/server/cmd/cache_proxy:cache_proxy_image', skip_latest_tag)

def get_cpu_architecture():
    arch = platform.machine()
    if arch in ['x86_64', 'AMD64']:
        return 'amd64'
    elif arch in ['aarch64', 'arm64', 'ARM64']:
        return 'arm64'
    else:
        die('unknown CPU architecture ' + arch)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--auto', default=False, action='store_true')
    parser.add_argument('--allow_dirty', default=False, action='store_true')
    parser.add_argument('--tag', default='', help='Version tag to create at HEAD and push, e.g. v2.310.0. Must not already exist.')
    parser.add_argument('--update_app_image', default=False, action='store_true')
    parser.add_argument('--update_enterprise_app_image', default=False, action='store_true')
    parser.add_argument('--update_executor_image', default=False, action='store_true')
    parser.add_argument('--update_proxy_image', default=False, action='store_true')
    parser.add_argument('--arch_specific_executor_tag', default=False, action='store_true', help='Suffix the executor image tag with the CPU architecture (amd64 or arm64)')
    parser.add_argument('--arch_specific_proxy_tag', default=False, action='store_true', help='Suffix the cache proxy image tag with the CPU architecture (amd64 or arm64)')
    parser.add_argument('--version', default='', help='Existing version tag, used when pushing docker images.')
    parser.add_argument('--skip_latest_tag', default=False, action='store_true')
    parser.add_argument('--mark_workspace_as_safe', default='')
    args = parser.parse_args()

    if args.mark_workspace_as_safe:
        run_or_die('git config --global --add safe.directory %s' % args.mark_workspace_as_safe)

    if workspace_is_clean():
        print("Workspace is clean!")
    elif args.allow_dirty:
        print("WARNING: Workspace contains uncommitted changes; ignoring due to --allow_dirty.")
    else:
        die('Your workspace has uncommitted changes. ' +
            'Please run this in a clean workspace!')

    if args.tag and args.version:
        die('At most one of --tag and --version may be set.')
    new_version = args.tag or args.version
    if not new_version:
        die('Pass --tag to create a new version tag, or --version to use an existing one.')

    if args.tag:
        if not VERSION_TAG_RE.match(args.tag):
            die(f'Invalid version tag {args.tag!r}: expected something like v2.310.0')
        if remote_tag_exists(new_version):
            die(f'Tag {new_version} already exists on origin. Bump the VERSION file on the release branch.')
        head = run_or_die('git log -1 --format="%h %s"', capture_stdout=True).stdout.strip()
        if not args.auto and not yes_or_no(f'Tag {head} as {new_version} and push it?'):
            die('Aborted.')
        create_and_push_tag(new_version)
        print("Pushed tag for new version %s" % new_version)

    # Write the version tag to $GITHUB_OUTPUT if it exists.
    github_outputs_file = os.environ.get('GITHUB_OUTPUT')
    if github_outputs_file:
        with open(github_outputs_file, 'a') as f:
            f.write('version_tag=' + new_version + '\n')
        print("Wrote version_tag output to $GITHUB_OUTPUT")

    images = []
    if args.update_app_image:
        images.append("buildbuddy-app-onprem")
    if args.update_enterprise_app_image:
        images.append("buildbuddy-app-enterprise")
    if args.update_executor_image:
        images.append("buildbuddy-executor-enterprise")
    if args.update_proxy_image:
        images.append("buildbuddy-proxy-enterprise")

    if images:
        print('Building and pushing docker images', images)
        update_docker_images(
            images, new_version, args.skip_latest_tag, args.arch_specific_executor_tag, args.arch_specific_proxy_tag
        )
    print("Done -- proceed with the release guide!")

if __name__ == "__main__":
    main()
