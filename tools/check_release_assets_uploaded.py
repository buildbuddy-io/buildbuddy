#!/usr/bin/env python3
import argparse
import os
import requests
import sys
import time

EXPECTED_ASSETS = [
    "executor-enterprise-darwin-arm64",
    "buildbuddy-enterprise-linux-amd64",
    "buildbuddy-linux-amd64",
    "executor-enterprise-linux-amd64",
    "buildbuddy-enterprise-darwin-amd64",
    "executor-enterprise-darwin-amd64",
    "executor-enterprise-windows-amd64-beta.exe",
    "buildbuddy-darwin-amd64"
]

def die(message):
    print(message)
    sys.exit(1)

def get_release(version_tag):
    github_token = os.environ.get('GITHUB_TOKEN')
    query_url = f"https://api.github.com/repos/buildbuddy-io/buildbuddy/releases?page=1"
    headers = {'Authorization': f'token {github_token}'}
    r = requests.get(query_url, headers=headers)
    if r.status_code == 401:
        die("Invalid github credentials. Did you pass a valid github token?")

    for release in r.json():
        if release["tag_name"] == version_tag:
            return release

    return None

# get_assets_for_release returns a list of URL strings representing the assets uploaded to the release with the given
# version tag
# Example output:
# ['https://github.com/buildbuddy-io/buildbuddy/releases/download/v2.0.0/executor-enterprise-darwin-arm64',
# 'https://github.com/buildbuddy-io/buildbuddy/releases/download/v2.0.0/buildbuddy-enterprise-linux-amd64']
def get_assets_for_release(version_tag):
    release = get_release(version_tag)
    if release is None:
        die(f"Release with tag {version_tag} not found.")

    asset_objs = release["assets"]
    asset_urls = [o["name"] for o in asset_objs]
    return asset_urls

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--polling_interval_sec', type=int, default=60)
    parser.add_argument('--timeout_sec', type=int, default=600)
    parser.add_argument('--release_version_tag', required=True)
    args = parser.parse_args()

    expected_assets = EXPECTED_ASSETS
    start_time_sec = time.time()
    elapsed_time_sec = 0
    while elapsed_time_sec < args.timeout_sec:
        print(f"Polling for release assets {expected_assets}.")
        asset_urls = get_assets_for_release(args.release_version_tag)
        missing_assets = []
        for expected_asset in expected_assets:
            asset_uploaded = False
            for uploaded_asset in asset_urls:
                if expected_asset in uploaded_asset:
                    asset_uploaded = True
                    break

            if not asset_uploaded:
                missing_assets.append(expected_asset)
        expected_assets = missing_assets

        if len(expected_assets) == 0:
            break

        time.sleep(args.polling_interval_sec)
        elapsed_time_sec = time.time() - start_time_sec

    if len(expected_assets) > 0:
        print(f"Missing assets {expected_assets}", file=sys.stderr)

if __name__ == "__main__":
    main()
