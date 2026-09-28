#!/usr/bin/env python3
import argparse
import os
import platform
import re
import requests
import subprocess
import sys
import time

"""
release.py - A simple script to create a release.

This script will do the following:

  1) Check that your working repository is clean.
  2) Compute a new version tag: minor bumps use the highest repository version;
     patch bumps use the release branch's own version. Create the tag at HEAD.
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

def is_published_release(version_tag):
    github_token = os.environ.get('GITHUB_TOKEN')
    # This API does not return draft releases
    query_url = f"https://api.github.com/repos/buildbuddy-io/buildbuddy/releases/tags/{version_tag}"
    headers = {'Authorization': f'token {github_token}'}
    r = requests.get(query_url, headers=headers)
    if r.status_code == 401:
        die("Invalid github credentials. Did you set the GITHUB_TOKEN environment variable?")
    elif r.status_code == 200:
        return True
    else:
        return False

def bump_patch_version(version):
    parts = version.split(".")
    patch_int = int(parts[-1])
    parts[-1] = str(patch_int +1)
    return ".".join(parts)

def bump_minor_version(version):
    parts = version.split(".")
    # Bump minor version
    minor_version = int(parts[-2])
    parts[-2] = str(minor_version +1)
    # Set patch version to 0
    parts[-1] = str(0)
    return ".".join(parts)

def yes_or_no(question):
    while "the answer is invalid":
        reply = input(question+" (y/n): ").lower().strip()
        if reply[:1] == "y":
            return True
        if reply[:1] == "n":
            return False

def is_valid_version(version):
    return version.startswith("v")

def get_version_override():
    version = input('What version do you want to release?\n').lower().strip()
    if is_valid_version(version):
        return version
    else:
        print("Invalid version: %s -- versions must start with 'v'" % version)
        return get_version_override()


def confirm_new_version(version):
    while not yes_or_no("Please confirm you want to release version %s" % version):
        version = get_version_override()
    return version

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

def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()

def remote_tag_commit(tag):
    refs = dict(
        line.split()[::-1]
        for line in git(
            "ls-remote", "origin", f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}"
        ).splitlines()
    )
    return refs.get(f"refs/tags/{tag}^{{}}", refs.get(f"refs/tags/{tag}"))

def check_remote_tag_owner(tag, head, branch):
    if not branch.startswith("bb_release_"):
        return
    # Fetch the remote object without updating the local tag: after a failed
    # racing push the local annotation could still describe our losing run.
    git("fetch", "--no-tags", "origin", f"refs/tags/{tag}")
    if (git("rev-parse", "FETCH_HEAD^{commit}") != head
            or git("cat-file", "-t", "FETCH_HEAD") != "tag"
            or git("cat-file", "tag", "FETCH_HEAD").splitlines()[-1] != f"Release-Branch: {branch}"):
        raise ValueError(f"{tag} does not belong to release branch {branch}")

def create_and_push_tag(old_version, new_version, release_notes='', branch=None):
    head = git("rev-parse", "HEAD")
    branch = branch or git("rev-parse", "--abbrev-ref", "HEAD")
    existing = remote_tag_commit(new_version)
    if existing == head:
        check_remote_tag_owner(new_version, head, branch)
        print(f"{new_version} already tags {head}")
        return
    if existing:
        raise ValueError(f"{new_version} already tags another commit: {existing}")

    commit_message = "Bump tag %s -> %s (release.py)" % (old_version, new_version)
    if len(release_notes) > 0:
        commit_message = "\n".join([commit_message, release_notes])

    # Several release branches can start at the same commit. Record ownership
    # so later patch pushes don't pick another branch's minor-version tag.
    if branch.startswith("bb_release_"):
        commit_message += f"\n\nRelease-Branch: {branch}"

    git("tag", "-a", new_version, head, "-m", commit_message)
    try:
        git("push", "origin", f"refs/tags/{new_version}:refs/tags/{new_version}")
    except subprocess.CalledProcessError:
        # Concurrent retries may race to push the same tag. Never move a tag;
        # accept the other run's result only if it tags the same event commit.
        if remote_tag_commit(new_version) != head:
            raise
        check_remote_tag_owner(new_version, head, branch)

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

def generate_release_notes(old_version):
    release_notes_cmd = 'git log --max-count=50 --pretty=format:"%ci %cn: %s"' + ' %s...HEAD' % old_version
    p = subprocess.Popen(release_notes_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    buf = ""
    while True:
        line = p.stdout.readline()
        if not line:
            break
        buf += line.decode("utf-8")
    return buf

def version_tuple(tag):
    return tuple(map(int, tag[1:].split(".")))

def get_version_tags(*filters, branch=None):
    tags = []
    for tag in git("tag", *filters, "-l", "v*").splitlines():
        if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag):
            continue
        if branch:
            # Lightweight tag contents are the *commit* message, not ownership
            # metadata. Only trust the final line of an annotated tag.
            if git("cat-file", "-t", f"refs/tags/{tag}") != "tag":
                continue
            contents = git("for-each-ref", "--format=%(contents)", f"refs/tags/{tag}")
            if not contents or contents.splitlines()[-1] != f"Release-Branch: {branch}":
                continue
        tags.append(tag)
    return tags

def get_latest_remote_version():
    git("fetch", "origin", "--tags")
    tags = get_version_tags()
    if not tags:
        raise ValueError("No vX.Y.Z version tags found")
    # A recent patch on an older release must not move the next minor backward.
    return max(tags, key=version_tuple)

def get_patch_base_version(branch, previous_commit=None, attempts=90, interval=10):
    if not re.fullmatch(r"bb_release_[A-Za-z0-9_-]+", branch):
        raise ValueError("Expected a bb_release_* branch")
    if git("rev-parse", "--is-shallow-repository") == "true":
        raise ValueError("Full history is required")
    if previous_commit:
        if not re.fullmatch(r"[0-9a-f]{40}", previous_commit):
            raise ValueError("Expected a full previous commit SHA")
        head = git("rev-parse", "HEAD")
        if previous_commit == head:
            raise ValueError("Expected a push with new commits")
        git("merge-base", "--is-ancestor", previous_commit, head)
        filters = ("--points-at", previous_commit)
    else:
        # Manual invocation on a named release branch uses its reachable tags.
        filters = ("--merged", "HEAD")
        attempts = 1

    for attempt in range(attempts):
        git("fetch", "origin", "--tags")
        tags = get_version_tags(*filters, branch=branch)
        if tags:
            if len({version_tuple(tag)[:2] for tag in tags}) != 1:
                raise ValueError(f"Ambiguous release version tags for {branch}")
            return max(tags, key=version_tuple)
        if attempt + 1 < attempts:
            # Waiting on the preceding push preserves order without a workflow
            # concurrency group, which could replace pending runs.
            print(f"Waiting for the release tag at previous head {previous_commit}", flush=True)
            time.sleep(interval)
    raise ValueError(
        f"No release version tag at {previous_commit or 'HEAD history'} for branch {branch}; "
        "repair the initial cut or previous push's tagging run, then rerun"
    )

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
    parser.add_argument('--force', default=False, action='store_true')
    parser.add_argument('--bump_version_type', default='minor', choices=['major', 'minor', 'patch', 'none'])
    parser.add_argument('--release_branch', default='', help='Release branch owning the tag; defaults to the checked-out branch. Required for patch bumps from a detached checkout.')
    parser.add_argument('--previous_commit', default='', help='Previous release-branch head for an automated patch push. Wait for its tag rather than skipping an earlier push.')
    parser.add_argument('--update_app_image', default=False, action='store_true')
    parser.add_argument('--update_enterprise_app_image', default=False, action='store_true')
    parser.add_argument('--update_executor_image', default=False, action='store_true')
    parser.add_argument('--update_proxy_image', default=False, action='store_true')
    parser.add_argument('--arch_specific_executor_tag', default=False, action='store_true', help='Suffix the executor image tag with the CPU architecture (amd64 or arm64)')
    parser.add_argument('--arch_specific_proxy_tag', default=False, action='store_true', help='Suffix the cache proxy image tag with the CPU architecture (amd64 or arm64)')
    parser.add_argument('--version', default='', help='Version tag override, used when pushing docker images. Implies --bump_version_type=none')
    parser.add_argument('--skip_latest_tag', default=False, action='store_true')
    parser.add_argument('--mark_workspace_as_safe', default='')
    args = parser.parse_args()
    if args.previous_commit and (args.bump_version_type != 'patch' or args.version or not args.auto):
        parser.error('--previous_commit requires --auto --bump_version_type=patch and cannot be combined with --version')
    if args.release_branch and not re.fullmatch(r"bb_release_[A-Za-z0-9_-]+", args.release_branch):
        parser.error('--release_branch must be a bb_release_* branch')

    if args.mark_workspace_as_safe:
        run_or_die('git config --global --add safe.directory %s' % args.mark_workspace_as_safe)

    if workspace_is_clean():
        print("Workspace is clean!")
    elif args.allow_dirty:
        print("WARNING: Workspace contains uncommitted changes; ignoring due to --allow_dirty.")
    else:
        die('Your workspace has uncommitted changes. ' +
            'Please run this in a clean workspace!')

    branch = args.release_branch or git("rev-parse", "--abbrev-ref", "HEAD")
    if args.bump_version_type == 'patch' and not args.version:
        old_version = get_patch_base_version(branch, args.previous_commit or None)
    else:
        old_version = get_latest_remote_version()

    # Automated tag creation is independent of Github release publication.
    if not args.force and not is_published_release(old_version):
        die(f"The latest tag {old_version} does not correspond to a published github release." +
        " It may be a draft release or it may have never been created." +
        " If you still want to upgrade the version, rerun the script with --force.")

    new_version = old_version
    if args.version:
        new_version = args.version
    elif args.bump_version_type != 'none':
        if args.bump_version_type == 'patch':
            new_version = bump_patch_version(old_version)
        elif args.bump_version_type == 'minor':
            new_version = bump_minor_version(old_version)
        else:
            die(f"Unimplemented bump version type: {args.bump_version_type}")

        release_notes = generate_release_notes(old_version)
        print("release notes:\n %s" % release_notes)
        print('I found existing version: %s' % old_version)
        if not args.auto:
            new_version = confirm_new_version(new_version)
        print("Ok, I'm doing it! bumping %s => %s..." % (old_version, new_version))

        create_and_push_tag(old_version, new_version, release_notes, branch=branch)
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
