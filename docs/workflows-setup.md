---
id: workflows-setup
title: Workflows setup
sidebar_label: Workflows setup
---

Workflows automatically build and test your code with
BuildBuddy whenever a commit is pushed to your GitHub repo.

When combined with GitHub's branch protection rules, workflows can help prevent
unwanted code (that doesn't build or pass tests) from being merged into the main branch.

Best of all, workflows let you run any Bazel commands you would like,
so you can leverage all the same BuildBuddy features that you get when
running Bazel locally, like the results UI, remote caching, remote execution.

## Enable workflows for a repo

### 1. Link GitHub account

If you use GitHub, make sure that your BuildBuddy org has a GitHub
org linked to it, using the "Link GitHub account" button from the
settings page.

### 2. Link the repository

Click **Workflows** in the BuildBuddy app and select a repo to
be linked to BuildBuddy.

Then, BuildBuddy will run `bazel test //...` whenever a commit is pushed to
your repo. It reports the status of the test as well as BuildBuddy links to
GitHub, which you can see on the repo's home page or in pull request branches.

## Configuring your workflow

To learn how to change the default configuration, see [workflows configuration](workflows-config.md).

## Setting up branch protection rules

After you have created a workflow and you've pushed at least one commit
to the repo, you can configure your repo so that branches cannot be
merged unless the workflow succeeds.

To do this, go to **Settings** > **Branches** and find **Branch protection rules**.
Then, you click **Add rule** (or edit an existing rule).

Select the box **Require status checks to pass before merging** and enable
the check corresponding to the BuildBuddy workflow (by default, this should
be **Test all targets**).

After you save your changes, pull requests will not be mergeable unless
the tests pass on BuildBuddy.

## Building in the workflow runner environment

BuildBuddy workflows execute using Docker on a recent Ubuntu base image
(Ubuntu 18.04 at the time of this writing), with some commonly used tools
and libraries pre-installed.

If you would like to test whether your build will succeed with
BuildBuddy workflows without having to set up and trigger the workflow,
you can instead run the image with Docker, clone your Git repo, and invoke
`bazel` to run your tests.

```bash
# Start a new shell inside the workflows environment (requires docker)
docker run --rm -it "gcr.io/flame-public/buildbuddy-ci-runner:latest"

# Clone your repo and test it
git clone https://github.com/acme-inc/acme
cd acme
bazel test //...
```

The Dockerfile we use to build the image (at `HEAD`) is [here](https://github.com/buildbuddy-io/buildbuddy/blob/master/enterprise/dockerfiles/ci_runner_image/Dockerfile).
