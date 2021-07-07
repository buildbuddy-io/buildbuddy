# Git webhooks

This directory contains parsers for Git webhooks.

## Manual testing

For manual testing instructions, see:
https://github.com/buildbuddy-io/buildbuddy-ci-playground

## Automated testing

Within each Git provider package in this directory, there is a `test_data` sub-directory
which contains webhook payloads. To generate (or re-generate) test data:

<!-- TODO: push the playground repo to other Git hosts and simplify these steps -->

1. Clone the CI playground repository: https://github.com/buildbuddy-io/buildbuddy-ci-playground
2. Set the `REPO_URL` env var in the `.env` file and point it to a repo that we support
   (e.g. a GitHub repo or a Bitbucket repo). If you don't set this var, it defaults to the
   playground repo itself (on GitHub).
3. Spin up your BuildBuddy enterprise server locally.
4. Run `./dev.sh`. It will prompt you for access tokens for the repo you've selected. It will
   automatically create a webhook for you and register it to the Git repo.
5. Create a no-op change that results in a Git diff: e.g. update the timestamp value in the BUILD
   file to an arbitrary number.
6. Trigger an event:
   - To trigger a `push` event, push your change using `git push`.
   - To trigger a `pull_request` event, create a new branch with your change, push the branch,
     and submit a pull request / merge request via the Git provider's website.
7. Visit your local `ngrok` proxy at http://localhost:4040 -- it should show the complete HTTP
   requests of the webhook payloads sent by the Git provider. Note the `Event` header (e.g.
   `X-Github-Event: pull_request`), which indicates the type of event. Copy the raw request body of
   the request into a `.json` file under the `test_data` folder, and give it a name corresponding
   to the event, for example: `pull_request.json`. You may need to convert the payload from
   URL-encoded form data to JSON. For this, you can use Chrome's devtools and run:
   ```js
   console.log(decodeURIComponent(`<PASTE-PAYLOAD-HERE>`.replace(/^payload=/, "")));
   ```
8. Anonymize the data: use find-and-replace and set the commit author's full name to `Test` and their
   email to `test@buildbuddy.io`.
