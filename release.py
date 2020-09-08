#!/usr/bin/env python
from __future__ import print_function

import json
import os
import re
import subprocess
import sys
import time
import tempfile

# Don't care about py2/py3 differences too much.
try:
    from urllib.parse import urlencode
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
except ImportError:
    from urllib import urlencode
    from urllib2 import urlopen, Request
    from urllib2 import HTTPError


"""
release.py - A simple script to create a release.
This script will do the following:
  1) Check that your working repository is clean. x
  2) Bump the version.
  3) Commit the bumped version.
  4) Create a tag in buildbuddy-io/buildbuddy at the new bumped version.
  5) Build any artifacts.
  6) Upload the artifacts to github to the repo.
  7) Finalize the release.
"""

def die(message):
    print(message)
    sys.exit(1)

def run_or_die(cmd):
    print("(debug) running cmd: %s" % cmd)
    p = subprocess.Popen(cmd,
                         shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    p.wait()
    rsp = "\n".join(p.stdout.readlines())
    print("(debug) response: %s" % rsp)
    if p.returncode != 0:
        print(p.returncode)
        die("Command failed with code %s: %s" % (p.returncode, rsp))

def workspace_is_clean():
    p = subprocess.Popen('git status --untracked-files=no --porcelain',
                         shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    return len(p.stdout.readlines()) == 0

def read_version(file):
    with open(file) as f:
        return f.read().replace('\n', '').replace('\t', '').strip()

def bump_patch_version(version):
    parts = version.split(".")
    patch_int = int(parts[-1])
    parts[-1] = str(patch_int +1)
    return ".".join(parts)

def yes_or_no(question):
    while "the answer is invalid":
        reply = str(raw_input(question+' (y/n): ')).lower().strip()
        if reply[:1] == 'y':
            return True
        if reply[:1] == 'n':
            return False

def confirm_new_version(version):
    while not yes_or_no("Please confirm you want to release version %s" % version):
        version = str(raw_input('What version do you want to release?\n')).lower().strip()
    return version

def update_version_in_file(old_version, new_version, version_file):
    edit_cmd = 'sed -i \'s/%s/%s/g\' %s' % (old_version, new_version, version_file)
    run_or_die(edit_cmd)

def commit_version_bump(old_version, new_version):
    commit_cmd = 'git commit -am "Bump version %s -> %s (release.py)"' % (old_version, new_version)
    run_or_die(commit_cmd)

def create_and_push_tag(old_version, new_version, release_notes=''):
    commit_message = "Bump tag %s -> %s (release.py)" % (old_version, new_version)
    if len(release_notes) > 0:
        commit_message = "\n".join([commit_message, release_notes])

    commit_msg_file = tempfile.NamedTemporaryFile(delete=False)
    commit_msg_file_name = commit_msg_file.name
    commit_msg_file.write(commit_message)
    commit_msg_file.close()

    tag_cmd = 'git tag -a %s -F "%s"' % (new_version, commit_msg_file_name)
    run_or_die(tag_cmd)
    push_tag_cmd = 'git push origin %s' % new_version
    run_or_die(push_tag_cmd)

def build_artifacts(repo_name, new_version):
    build_cmd = 'git archive --format=tar.gz -o /tmp/%s.tar.gz --prefix=%s-%s/ master' % (repo_name, repo_name, new_version)
    run_or_die(build_cmd)
    return "/tmp/%s.tar.gz" % repo_name

def update_docker_image(new_version):
    version_build_cmd = 'bazel run -c opt --stamp --define version=server-image-%s --define release=true deployment:release_onprem' % new_version
    run_or_die(version_build_cmd)
    latest_build_cmd = 'bazel run -c opt --stamp --define version=latest --define release=true deployment:release_onprem'
    run_or_die(latest_build_cmd)

def generate_release_notes(old_version):
    release_notes_cmd = 'git log --max-count=50 --pretty=format:"%ci %cn: %s"' + ' %s...HEAD' % old_version
    p = subprocess.Popen(release_notes_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return "".join(p.stdout.readlines())

def github_make_request(
        auth_token=None,
        repo=None,
        data=None,
        extra_headers=None,
        path='',
        subdomain='api',
        url_params=None,
        **extra_request_args
):
        headers = {'Accept': 'application/vnd.github.v3+json'}
        if extra_headers is not None:
            headers.update(extra_headers)
        if auth_token is not None:
            headers['Authorization'] = 'token ' + auth_token
        if url_params is not None:
            path += '?' + urlencode(url_params)
        request = Request(
            'https://' + subdomain + '.github.com/repos/' + repo + path,
            headers=headers,
            data=data,
            **extra_request_args
        )
        response_body = urlopen(request).read().decode()
        if response_body:
            _json = json.loads(response_body)
        else:
            _json = {}
        return _json

def get_or_create_release(repo, version):
    token = os.environ['GITHUB_TOKEN']
    tag = version
    # Check if the release already exists?
    try:
        _json = github_make_request(auth_token=token, repo=repo, path='/releases/tags/' + tag)
    except HTTPError as e:
        if e.code != 404:
            raise e
    else:
        return _json['id']

    # Create the release if not.
    _json = github_make_request(
        auth_token=token,
        repo=repo,
        data=json.dumps({
            'tag_name': tag,
            'name': tag,
            'prerelease': True,
        }).encode(),
        path='/releases'
    )
    return _json['id']    

def create_release_and_upload_artifacts(repo, version, artifacts):
    artifact_set = set(artifacts)
    release_id = get_or_create_release(repo, version)
    token = os.environ['GITHUB_TOKEN']
    
    # Clear the prebuilts for a upload.
    _json = github_make_request(
        repo=repo,
        path=('/releases/' + str(release_id) + '/assets'),
    )
    for asset in _json:
        if asset['name'] in artifact_set:
            _json = github_make_request(
                auth_token=token,
                repo=repo,
                path=('/releases/assets/' + str(asset['id'])),
                method='DELETE',
            )
            break
        
    # Upload the artifacts.
    for artifact in artifacts:
        name = os.path.split(artifact)[-1]
        with open(artifact, 'rb') as myfile:
            content = myfile.read()
            _json = github_make_request(
                auth_token=token,
                repo=repo,
                data=content,
                extra_headers={'Content-Type': 'application/zip'},
                path=('/releases/' + str(release_id) + '/assets'),
                subdomain='uploads',
                url_params={'name': name},
            )

def main():
    if not workspace_is_clean():
        die('Your workspace has uncommitted changes. ' +
            'Please run this in a clean workspace!')
    gh_token = os.environ.get('GITHUB_TOKEN')
    if not gh_token or gh_token == '':
        die('GITHUB_TOKEN env variable not set. Please go get a repo_token from'
            ' https://github.com/settings/tokens')

    version_file = 'VERSION'
    org_name = "buildbuddy-io"
    repo_name = "buildbuddy"

    old_version = read_version(version_file)
    new_version = bump_patch_version(old_version)
    release_notes = generate_release_notes(old_version)
    print("release notes:\n" + release_notes)
    print('I found existing version: %s' % old_version)
    new_version = confirm_new_version(new_version)
    print("Ok, I'm doing it! bumping %s => %s..." % (old_version, new_version))

    time.sleep(2)
    update_version_in_file(old_version, new_version, version_file)
    commit_version_bump(old_version, new_version)
    create_and_push_tag(old_version, new_version, release_notes)

    update_docker_image(new_version)

    ## Don't need this because github automatically creates a source archive when we
    ## make a new tag. Useful when we have artifacts to upload.
    # artifacts = build_artifacts(repo_name, new_version)
    # create_release_and_upload_artifacts("/".join(org_name, repo_name), new_version, artifacts)

    print("Release (%s) complete. Go enjoy a cold one!" % new_version)

if __name__ == "__main__":
    main()
