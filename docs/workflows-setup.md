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

There are two ways to enable workflows for a GitHub repository.

### Method 1: Link a GitHub account

BuildBuddy allows linking a GitHub account to your organization. When
linking repositories using your linked GitHub account, workflows will be
authorized using the GitHub access token associated with your linked
account.

If you have multiple members in your BuildBuddy organization, we recommend
linking a **machine account** rather than a personal account, since the
token granted to the account may be accessed by other members of the
organization.

The linked account must be an **owner** of any repositories that you would
like to link to BuildBuddy.

To link a GitHub account:

1. Navigate to the **Settings** page in the BuildBuddy app.
1. Click the **GitHub link** tab.
1. Click **Link GitHub account** and follow the instructions to link an
   account.
1. Once you've linked your account, click **Workflows** in the BuildBuddy
   app and select a repo to be linked to BuildBuddy.

If you don't see the desired repo in the list, click **Enter details
manually,** and enter the repository URL. Leave the access token field
blank to use the token associated with your linked account.

### Method 2: Use a personal access token

You can also link a single GitHub repository by providing a personal access
token to be used only for workflow executions for that repository.

If you have multiple members in your BuildBuddy organization, we recommend
generating the access token using a **machine account** rather than your
personal account, since the token granted to the account may be accessed
by other members of the organization.

The account used to generate the token must be an **owner** of the
repository that you are attempting to link.

To link a repository using a personal access token:

1. Ensure that you are logged into GitHub using the desired account.
1. Navigate to https://github.com/settings/tokens
1. Generate a new **classic** token, granting all **repo** permissions
   as well as all **admin:repo_hook** permissions.
1. Click **Workflows** in the BuildBuddy app.
1. If you have a GitHub account already linked to your organization, you
   will see a list of repositories accessible to the linked account. Click
   **Enter details manually** to bypass this selection.
1. Enter the repository URL and personal access token, then click
   **Create**.

## Running workflows

Once a repository is linked, BuildBuddy will automatically run `bazel test //...` whenever a commit is pushed to your repo. It will report commit
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
