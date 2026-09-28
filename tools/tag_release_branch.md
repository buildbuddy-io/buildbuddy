# Release branch tags

The `buildbuddy-internal` cut-release workflow creates a `bb_release_*` branch
and its initial minor-version tag (for example, `v2.150.0`).
`tag-release-branch.yaml` skips that creation push and tags each subsequent
fast-forward push with the next patch version. Both use `release.py`:

```sh
# At release branch creation (on the named release branch):
./release.py --auto --force --bump_version_type=minor

# On each subsequent push (checked out at the push event's exact commit):
./release.py --auto --force --bump_version_type=patch \
  --release_branch="$RELEASE_BRANCH" --previous_commit="$BEFORE"
```

Minor bumps use the highest numeric version in the repository, not the most
recently created tag. A recent patch to an older minor cannot regress the next
minor version. Manual patch bumps without `--previous_commit` use the highest
reachable version owned by the checked-out release branch (or `--release_branch`
for a detached checkout).

The patch version comes from the tag on the push's **previous head**, not the
latest tag in the repository. A push to `v2.150.0` becomes `v2.150.1` even if a
newer release branch already has `v2.151.0`. A multi-commit push creates one tag,
at the push's final commit.

Version tags created on release branches carry a `Release-Branch` annotation.
This distinguishes branches even when multiple release cuts start at the same
public commit. Tags without this annotation are not used by the workflow:
even a sole legacy tag could belong to an older cut at that same commit.

Overlapping runs wait up to roughly 15 minutes for the preceding head's tag.
Retries reuse an existing tag only if it points to the expected commit. Tags
are never moved, and force pushes are rejected. If the preceding tag is missing
(for example, an earlier tagging run failed or was skipped), repair/rerun that
earlier run and then rerun the failed dependent runs in push order.

Both the workflow and updated `release.py` must be included in the release
branch. Merge this change before cutting new branches. Existing release branches
are not automatically migrated: they need this workflow/script and a version
tag with the matching branch annotation at the head preceding the push.

This workflow only creates git tags. It uses the existing release-bot token, so
the repository's existing tag-triggered release/artifact workflows still run;
this change neither modifies nor disables them.

Run the local integration tests with:

```sh
python3 -m unittest discover -s tools -p tag_release_branch_test.py
```
