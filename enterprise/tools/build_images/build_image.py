#!/usr/bin/env python

import argparse
import itertools
import os
import subprocess
from lib.check_buildx_support import check_buildx_support

def main():
    class ArgsNamespace(argparse.Namespace):
        registry: str
        repository: str
        no_push: bool
        force_ignore_new_repository_check: bool
        tag: str
        suffix: str
        force_ignore_prod_checks: bool
        dockerfile: str
        context_dir: str
        platforms: list[str]
        buildx_container_name: str

    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--registry",
            default="gcr.io/flame-build",
            type=str,
            help="Container registry where the target repository is located.",
            dest="registry",
            )
    parser.add_argument(
            "--repository",
            type=str,
            required=True,
            help="Name of the target repository in the container registry ('-dev' and '-prod' suffixes should be appended automatically).",
            dest="repository",
            )
    parser.add_argument(
            "--no-push",
            action="store_true",
            default=False,
            help="Set to only build the image and cache it in docker's build cache. To push the built image, run the build command again with '--push'.",
            dest="no_push",
            )
    parser.add_argument(
            "--force-ignore-new-repository-check",
            action="store_true",
            default=False,
            help="Set to ignore warnings about the repository not currently existing in the registry. Set this if you really are looking to push a repository with an entirely new name, not just a new image to an existing repository.",
            dest="force_ignore_new_repository_check",
            )
    parser.add_argument(
            "--tag",
            default="",
            type=str,
            help="Tag to use in the target repository for the built image (if any).",
            dest="tag",
            )
    suffix_group = parser.add_argument_group("Suffixes", "Suffixes to be attached to the end of the repository name. Only one may be specified; '--dev' is the default.").add_mutually_exclusive_group()
    suffix_group.add_argument(
            "--dev",
            action="store_const",
            const="dev",
            default="dev",
            help="Adds '-dev' to the end of the repository name. Intended to be used for pushing dev images.",
            dest="suffix",
            )
    suffix_group.add_argument(
            "--prod",
            action="store_const",
            const="prod",
            help="Adds '-prod' to the end of the repository name. Intended to be used for pushing prod images.",
            dest="suffix",
            )
    suffix_group.add_argument(
            "--no-suffix",
            action="store_const",
            const="",
            help="No suffix will be added to the repository name. You will still receive a warning if you are pushing to a repository name that ends in '-prod'.",
            dest="suffix",
            )

    parser.add_argument(
            "--force-ignore-prod-checks",
            action="store_true",
            default=False,
            help="*** THIS OPTION IS DANGEROUS, BE SURE YOU KNOW WHAT YOU'RE DOING *** Set to ignore warnings and checks surrounding prod pushes. Recommended only in unmonitored scripts and in the case that the existing prod image is currently broken.",
            dest="force_ignore_prod_checks",
            )
    parser.add_argument(
            "--dockerfile",
            default="Dockerfile",
            type=str,
            help="Path to the dockerfile to use when building the image.",
            dest="dockerfile",
            )
    parser.add_argument(
            "--context-dir",
            type=str,
            help="Path to the context directory to use when building the image. Defaults to the dockerfile directory if not provided.",
            dest="context_dir",
            )
    parser.add_argument(
            "--platform",
            action="append",
            type=str,
            help="Repeated option. Set this to specify platforms to build the image for.",
            dest="platforms",
            )
    parser.add_argument(
            "--buildx-container-name",
            default="buildbuddy-buildx",
            type=str,
            help="Name of buildx container to use for building the images. If a buildx container with that name does not yet exist, it will be created.",
            dest="buildx_container_name",
            )

    args = parser.parse_args(namespace=ArgsNamespace)

    if not check_buildx_support():
        return 1

    context_dir = "."
    if args.context_dir != None:
        context_dir = args.context_dir
    elif os.path.dirname(os.path.abspath(args.dockerfile)) != "":
        context_dir = os.path.dirname(os.path.abspath(args.dockerfile))

    repository_path = f"{args.repository}"
    if args.suffix:
        repository_path = f"{repository_path}-{args.suffix if args.suffix else ''}"

    if not args.no_push and not args.force_ignore_prod_checks and str(repository_path).endswith("-prod"):
        response = input(f"Repository path {repository_path} ends in '-prod', indicating that this is a production image. Are you sure you want to push this image to a production repository? (y/N): ")
        if response and "yes".startswith(response.lower()):
            pass
        else:
            return 0

    if not args.no_push and not args.force_ignore_new_repository_check:
        completed_process = subprocess.run(["docker", "manifest", "inspect", repository_path], capture_output=True)
        if completed_process != 0:
            response = input(f"Repository {repository_path}  does not yet exist in {args.registry}. Are you sure you want to create a new repository? (y/N): ")
            if response and "yes".startswith(response.lower()):
                pass
            else:
                return 0

    completed_process = subprocess.run(["docker", "buildx", "inspect", args.buildx_container_name], capture_output=True)
    if completed_process.returncode == 1:
        print(f"No buildx container named {args.buildx_container_name}, creating it...")
        completed_process = subprocess.run(["docker", "buildx", "create", "--name", args.buildx_container_name])
        if completed_process.returncode == 0:
            print("Success!")
        else:
            "Failed to create buildx container."

    platform_args = zip(itertools.repeat("--platform"), args.platforms)
    flattened_platform_args = [arg for arg_pair in platform_args for arg in arg_pair]
    print(args.platforms)
    print(flattened_platform_args)
    completed_process = subprocess.run(
            [
                "docker", "buildx",
                "--builder", str(args.buildx_container_name),
                "build",
                "-t", f"{args.registry}/{f'{repository_path}:{args.tag}' if args.tag else repository_path}",
            ] + (
                ["-f", args.dockerfile] if args.dockerfile else []
                ) +
            flattened_platform_args + (
                [] if args.no_push else ["--push"]
                ) +
            [
                context_dir,
            ]
            )
    print(f"Executed {completed_process.args}")
    return completed_process.returncode

main()
