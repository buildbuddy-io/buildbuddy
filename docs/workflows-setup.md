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

To enable workflows, take the following steps:

1. Log in to the BuildBuddy app, and use the organization picker in the
   sidebar to select your preferred organization.
1. Navigate to the **Workflows** page using the sidebar.
1. Click **Link a repository**, then follow the steps displayed in the app.

## Running workflows

Once a repository is linked, BuildBuddy will automatically run `bazel test //...` whenever a commit is pushed to your repo's default branch
or whenever a pull request branch is updated. It will report commit
statuses to GitHub, which you can see on the repo's home page or in pull
request branches. The "Details" links in these statuses point to the
BuildBuddy UI, where you can see the result of the workflow run.

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

BuildBuddy workflows execute using a Firecracker MicroVM on an Ubuntu
18.04-based image, with some commonly used tools and libraries
pre-installed.

If you would like to test whether your build will succeed on
BuildBuddy workflows without having to set up and trigger the workflow,
you can get a good approximation of the workflow VM environment by running
the image locally with Docker, cloning your Git repo, and invoking
`bazel` to run your tests:

```bash
# Start a new shell inside the workflows Ubuntu 18.04 environment (requires docker)
docker run --rm -it "gcr.io/flame-public/buildbuddy-ci-runner:latest"

# Clone your repo and test it
git clone https://github.com/acme-inc/acme
cd acme
bazel test //...
```

The Dockerfile we use to build the image (at `HEAD`) is [here](https://github.com/buildbuddy-io/buildbuddy/blob/master/enterprise/dockerfiles/ci_runner_image/Dockerfile).

If you plan to use the Ubuntu 20.04 image (requires [advanced configuration](workflows-config#linux-image-configuration)), use
`"gcr.io/flame-public/rbe-ubuntu20-04-workflows:latest"` in the command
above.
