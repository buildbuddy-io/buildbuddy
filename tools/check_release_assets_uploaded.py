#!/usr/bin/env python3
import argparse
import os
import requests
import sys
import time

def die(message):
    print(message)
    sys.exit(1)

# get_assets_for_release returns a list of URL strings representing the assets uploaded to the release with the given
# version tag
# Example output:
# ['https://github.com/buildbuddy-io/buildbuddy/releases/download/v2.0.0/executor-enterprise-darwin-arm64',
# 'https://github.com/buildbuddy-io/buildbuddy/releases/download/v2.0.0/buildbuddy-enterprise-linux-amd64']
def get_assets_for_release(version_tag):
    github_token = os.environ.get('GITHUB_TOKEN')
    query_url = "https://api.github.com/graphql"
    json = {"query": "query {repository(owner: \"buildbuddy-io\", name: \"buildbuddy\") { release(tagName:\"" + version_tag+ "\"){ releaseAssets(last:10) { edges {node {downloadUrl}}}}}}"}
    headers = {'Authorization': f'token {github_token}'}
    r = requests.post(query_url, json=json, headers=headers)
    if r.status_code == 401:
        die("Invalid github credentials. Did you set the GITHUB_TOKEN environment variable?")

    release = r.json()["data"]["repository"]["release"]
    if release is None:
        die(f"Release with tag {version_tag} not found.")

    asset_objs = release["releaseAssets"]["edges"]
    asset_urls = [o["node"]["downloadUrl"] for o in asset_objs]
    return asset_urls

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--polling_interval_sec', type=int, default=60)
    parser.add_argument('--max_timeout_sec', type=int, default=600)
    parser.add_argument('--release_version_tag', required=True)
    args = parser.parse_args()

    expected_assets = [
        "executor-enterprise-darwin-arm64",
        "buildbuddy-enterprise-linux-amd64",
        "buildbuddy-linux-amd64",
        "executor-enterprise-linux-amd64",
        "buildbuddy-enterprise-darwin-amd64",
        "executor-enterprise-darwin-amd64",
        "buildbuddy-darwin-amd64"
    ]

    time_passed = 0
    while time_passed < args.max_timeout_sec:
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
        time_passed += args.polling_interval_sec
        print(f"Still missing release assets {expected_assets}. {time_passed}s have passed...")

    if len(expected_assets) > 0:
        print(f"Missing assets {expected_assets}", file=sys.stderr)
        exit(1)

if __name__ == "__main__":
    main()
