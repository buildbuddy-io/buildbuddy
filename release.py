#!/usr/bin/env python3
import argparse
import re
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

def bump_patch_version(version):
    parts = version.split(".")
    patch_int = int(parts[-1])
    parts[-1] = str(patch_int +1)
    return ".".join(parts)

def yes_or_no(question):
    while "the answer is invalid":
        reply = input(question+" (y/n): ").lower().strip()
        if reply[:1] == "y":
            return True
        if reply[:1] == "n":
            return False

def confirm_new_version(version):
    while not yes_or_no("Please confirm you want to release version %s" % version):
        version = input('What version do you want to release?\n').lower().strip()
    return version

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

def push_image(target, image_tag):
    command = (
        'bazel run -c opt --stamp '+
        '--define=release=true '+
        '--//deployment:image_tag={image_tag} '+
        '{target}'
    ).format(image_tag=image_tag, target=target)
    run_or_die(command)

def update_docker_images(version_tag, update_latest_tag):
    clean_cmd = 'bazel clean --expunge'
    run_or_die(clean_cmd)

    # OSS app
    push_image('//deployment:release_onprem', image_tag=version_tag)
    # Enterprise app
    push_image('//enterprise/deployment:release_enterprise', image_tag='enterprise-'+version_tag)
    # Enterprise executor
    push_image('//enterprise/deployment:release_executor_enterprise', image_tag='enterprise-'+version_tag)

    # update "latest" tags
    if update_latest_tag:
        # OSS app
        push_image('//deployment:release_onprem', image_tag='latest')
        # Enterprise app
        push_image('//enterprise/deployment:release_enterprise', image_tag='latest')
        # Enterprise executor
        push_image('//enterprise/deployment:release_executor_enterprise', image_tag='latest')


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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--allow_dirty', default=False, action='store_true')
    parser.add_argument('--skip_version_bump', default=False, action='store_true')
    parser.add_argument('--skip_latest_tag', default=False, action='store_true')
    args = parser.parse_args()

    bump_version = not args.skip_version_bump
    update_latest_tag = not args.skip_latest_tag

    if workspace_is_clean():
        print("Workspace is clean!")
    elif args.allow_dirty:
        print("WARNING: Workspace contains uncommitted changes; ignoring due to --allow_dirty.")
    else:
        die('Your workspace has uncommitted changes. ' +
            'Please run this in a clean workspace!')

    old_version = get_latest_remote_version()
    new_version = old_version
    if bump_version:
        new_version = bump_patch_version(old_version)
        release_notes = generate_release_notes(old_version)
        print("release notes:\n %s" % release_notes)
        print('I found existing version: %s' % old_version)
        new_version = confirm_new_version(new_version)
        print("Ok, I'm doing it! bumping %s => %s..." % (old_version, new_version))

        time.sleep(2)
        create_and_push_tag(old_version, new_version, release_notes)
        print("Pushed tag for new version %s" % new_version)

    update_docker_images(new_version, update_latest_tag)
    print("Pushed docker images for new version %s" % new_version)
    print("Done -- proceed with the release guide!")

if __name__ == "__main__":
    main()
