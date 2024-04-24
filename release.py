#!/usr/bin/env python3
import argparse
import os
import platform
import requests
import subprocess
import sys
import tempfile
import time

"""
release.py - A simple script to create a release.

This script will do the following:

  1) Check that your working repository is clean.
  2) Compute a new version tag by bumping the latest remote version tag,
     and create the tag pointing at HEAD.
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
    p = subprocess.Popen('git status --untracked-files=no --porcelain',
                         shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    return len(p.stdout.readlines()) == 0

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

def get_image(project, tag):
    query_url = f"https://gcr.io/v2/{project}/manifests/{tag}"
    r = requests.get(query_url)
    if r.status_code == 404:
        return None
    return r.json()

def create_and_push_tag(old_version, new_version, release_notes=''):
    commit_message = "Bump tag %s -> %s (release.py)" % (old_version, new_version)
    if len(release_notes) > 0:
        commit_message = "\n".join([commit_message, release_notes])

    commit_msg_file = tempfile.NamedTemporaryFile(mode='w+', delete=False)
    commit_msg_file_name = commit_msg_file.name
    commit_msg_file.write(commit_message)
    commit_msg_file.close()

    tag_cmd = 'git tag -a %s -F "%s"' % (new_version, commit_msg_file_name)
    run_or_die(tag_cmd)
    push_tag_cmd = 'git push origin %s' % new_version
    run_or_die(push_tag_cmd)

def push_image_for_project(project, version_tag, bazel_target, skip_update_latest_tag):
    version_image = get_image(project, version_tag)
    if version_image is None:
        build_image_with_bazel(bazel_target)
        tag_and_push_image_with_docker(bazel_target, project, version_tag)
    else:
        print(f'Image gcr.io/{project}:{version_tag} already exists, skipping bazel build.')

    if skip_update_latest_tag:
        return

    version_image = version_image or get_image(project, version_tag)
    if version_image is None:
        die(f"Could not fetch image with tag {version_tag} from project {project}.")

    latest_image = get_image(project, "latest")
    if latest_image is None:
        die(f"Could not fetch image with latest tag from project {project}.")

    should_update_latest_tag = version_image["config"]["digest"] != latest_image["config"]["digest"]
    if should_update_latest_tag:
        add_tag_cmd = f"echo 'yes' | gcloud container images add-tag gcr.io/{project}:{version_tag} gcr.io/{project}:latest"
        run_or_die(add_tag_cmd)

def build_image_with_bazel(bazel_target):
    print(f"Building docker image target {bazel_target}")
    # Note: we are not using container_push targets here, because it has a bug
    # where it uses "application/vnd.oci.image.layer.v1.tar" mediaType for some
    # image layers on arm64, which podman and containerd cannot handle.
    # https://github.com/buildbuddy-io/buildbuddy-internal/issues/3316
    run_or_die(f'bazel run -c opt --stamp --define=release=true {bazel_target}')

def tag_and_push_image_with_docker(bazel_target, project, version_tag):
    # rules_docker uses a convention where "//PACKAGE:LABEL" gets locally tagged
    # with "bazel/PACKAGE:LABEL".
    local_image_ref = bazel_target.replace('//', 'bazel/')
    remote_image_ref = f'gcr.io/{project}:{version_tag}'
    print(f'Tagging and pushing {remote_image_ref}')
    run_or_die(f'docker tag {local_image_ref} {remote_image_ref}')
    run_or_die(f'docker push {remote_image_ref}')

def update_docker_images(images, version_tag, skip_update_latest_tag, arch_specific_executor_tag):
    clean_cmd = 'bazel clean --expunge'
    run_or_die(clean_cmd)

    # OSS app
    if 'buildbuddy-app-onprem' in images:
        push_image_for_project("flame-public/buildbuddy-app-onprem", version_tag, '//server/cmd/buildbuddy:buildbuddy_image', skip_update_latest_tag)
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

def get_latest_remote_version():
    run_or_die('git fetch --all --tags')
    p = run_or_die("./tools/latest_version_tag.sh", capture_stdout=True)
    return p.stdout.strip()

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
    parser.add_argument('--update_app_image', default=False, action='store_true')
    parser.add_argument('--update_enterprise_app_image', default=False, action='store_true')
    parser.add_argument('--update_executor_image', default=False, action='store_true')
    parser.add_argument('--arch_specific_executor_tag', default=False, action='store_true', help='Suffix the executor image tag with the CPU architecture (amd64 or arm64)')
    parser.add_argument('--version', default='', help='Version tag override, used when pushing docker images. Implies --bump_version_type=none')
    parser.add_argument('--skip_latest_tag', default=False, action='store_true')
    args = parser.parse_args()

    if workspace_is_clean():
        print("Workspace is clean!")
    elif args.allow_dirty:
        print("WARNING: Workspace contains uncommitted changes; ignoring due to --allow_dirty.")
    else:
        die('Your workspace has uncommitted changes. ' +
            'Please run this in a clean workspace!')

    old_version = get_latest_remote_version()
    is_old_version_published = is_published_release(old_version)

    if not is_old_version_published and not args.force:
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

        time.sleep(2)
        create_and_push_tag(old_version, new_version, release_notes)
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

    if images:
        print('Building and pushing docker images', images)
        update_docker_images(
            images, new_version, args.skip_latest_tag, args.arch_specific_executor_tag
        )
    print("Done -- proceed with the release guide!")

if __name__ == "__main__":
    main()
