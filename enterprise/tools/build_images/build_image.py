#!/usr/bin/env python
"""
Script to build multi-platform docker images.
"""

import argparse
import os
import subprocess
import sys

from lib.check_buildx_support import check_buildx_support

def parse_program_arguments():
    """Parses the program arguments and returns a namespace representing them."""
    class ArgsNamespace(argparse.Namespace):
        registry: str
        repository: str
        push: bool
        do_new_repository_check: bool
        tag: str
        suffix: str
        do_prod_checks: bool
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
            action="store_false",
            default=True,
            help="Set to only build the image and cache it in docker's build cache. To push the built image, run the build command again with '--push'.",
            dest="push",
            )
    parser.add_argument(
            "--force-ignore-new-repository-check",
            action="store_false",
            default=True,
            help="Set to ignore warnings about the repository not currently existing in the registry. Set this if you really are looking to push a repository with an entirely new name, not just a new image to an existing repository.",
            dest="do_new_repository_check",
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
            action="store_false",
            default=True,
            help="*** THIS OPTION IS DANGEROUS, BE SURE YOU KNOW WHAT YOU'RE DOING *** Set to ignore warnings and checks surrounding prod pushes. Recommended only in unmonitored scripts and in the case that the existing prod image is currently broken.",
            dest="do_prod_checks",
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

    return parser.parse_args(namespace=ArgsNamespace)

def set_up_buildx_container(container_name, stdout=sys.stdout, stderr=sys.stderr):
    """Sets up a buildx container with the given name."""
    completed_process = subprocess.run(["docker", "buildx", "inspect", container_name], capture_output=True)
    if completed_process.returncode == 1:
        print(f"No buildx container named {container_name}, creating it...", file=stdout)
        completed_process = subprocess.run(["docker", "buildx", "create", "--name", container_name])
        if completed_process.returncode == 0:
            print("Success!", stdout)
        else:
            print("Failed to create buildx container.", stderr)

def yes_no_prompt(prompt, default_yes=False):
    """Prompts the user for a yes or no response. Returns True for "yes" and False for "no"."""
    try:
        response = input(f"{prompt} ({'Y/n' if default_yes else 'y/N'}): ")
    except EOFError:
        return default_yes
    if not response:
        return default_yes
    if default_yes:
        return not "no".startswith(response.lower())
    else:
        return "yes".startswith(response.lower())

def perform_prod_checks(repository_path: str):
    """Checks to make sure any push to prod is intentional."""
    if repository_path.endswith("-prod"):
        return yes_no_prompt(f"Repository path {repository_path} ends in '-prod', indicating that this is a production image. Are you sure you want to push this image to a production repository?")
    return True

def perform_new_repository_check(registry: str, repository_path: str):
    """Checks to make sure any creation of a new repository is intentional."""
    completed_process = subprocess.run(["docker", "manifest", "inspect", f"{registry}/{repository_path}"], capture_output=True)
    if completed_process != 0:
        return yes_no_prompt(f"Repository {repository_path} does not yet exist in {registry}. Are you sure you want to create a new repository?")
    return True

def repeated_opts(opt: str, params: list[str]):
    """Returns a list of the form [opt, params[0], opt, params[1], ... ]"""
    return [arg for param in params for arg in (opt, param)]

def main():
    args = parse_program_arguments()
    if not check_buildx_support():
        return 1

    context_dir = "."
    if args.context_dir:
        context_dir = args.context_dir
    elif os.path.dirname(os.path.abspath(args.dockerfile)) != "":
        context_dir = os.path.dirname(os.path.abspath(args.dockerfile))

    repository_path = f"{args.repository}{'-' + args.suffix if args.suffix else ''}"

    if args.push:
        if args.do_prod_checks and not perform_prod_checks(repository_path):
            print("Exiting...", file=sys.stdout)
            return 0

        if args.do_new_repository_check and not perform_new_repository_check(args.registry, repository_path):
            print("Exiting...", file=sys.stdout)
            return 0

    set_up_buildx_container(args.buildx_container_name)

    docker_build_args = (
            ["docker", "buildx"] +
            ["--builder", str(args.buildx_container_name)] +
            ["build"] +
            ["--tag", f"{args.registry}/{repository_path}{':' + args.tag if args.tag else ''}"] +
            (["--file", args.dockerfile] if args.dockerfile else []) +
            repeated_opts("--platform", args.platforms) +
            (["--push"] if args.push else []) +
            [context_dir]
            )
    completed_process = subprocess.run(docker_build_args)
    print(f"Executed command:\n{completed_process.args}", file=sys.stdout)
    return completed_process.returncode

main()
