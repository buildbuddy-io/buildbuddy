---
id: guide-auth
title: Authentication Guide
sidebar_label: Authentication Guide
---

You have two choices for authenticating your BuildBuddy requests:

- API key
- certificate based mTLS auth

Both of these choices require you to [create a BuildBuddy account](https://app.buildbuddy.io/).

## API key

This the simpler of the two methods. It passes an API key along with all grpcs requests that is associated with your BuildBuddy organization. This key can be used by anyone in your organization, as it ties builds to your org - not your individual user.

You can find API key authenticated URLs on your [setup instructions](https://app.buildbuddy.io/docs/setup/) once you've [created an account](https://app.buildbuddy.io/) and logged in.

These URLs can be added directly to your `.bazelrc` as long as no one outside of your organization has access to your source code.

If people outside of your organization have access to your source code (open source projects, etc) - you'll want to pull your credentials into a separate file that is only accessible by members of your organization and/or your CI machines. Alternatively, you can store your API key in an environment variable / secret and pass these flags in manually or with a wrapper script.

### Separate auth file

Using the `try-import` directive in your `.bazelrc` - you can direct bazel to pull in additional bazel configuration flags from a different file if the file exists (if the file does not exist, this directive will be ignored).

You can then place a second `auth.bazelrc` file in a location that's only accessible to members of your organization:

```
build --bes_backend=grpcs://YOUR_API_KEY@cloud.buildbuddy.io
build --remote_cache=grpcs://YOUR_API_KEY@cloud.buildbuddy.io
build --remote_executor=grpcs://YOUR_API_KEY@cloud.buildbuddy.io
```

And add a `try-import` to your main `.bazelrc` file at the root of your `WORKSPACE`:

```
try-import /path/to/your/auth.bazelrc
```

### Command line

The command line method allows you to store your API key in an environment variable or Github secret, and then pass authenticated flags in either manually or with a wrapper script.

If using Github secrets - you can create a secret called `BUILDBUDDY_API_KEY` containing your API key, then use that in your workflows:

```
bazel build --config=remote --bes_backend=${BUILDBUDDY_API_KEY}@cloud.buildbuddy.io --remote_cache=${BUILDBUDDY_API_KEY}@cloud.buildbuddy.io --remote_executor=${BUILDBUDDY_API_KEY}@cloud.buildbuddy.io
```

## Certificate

The other option for authenticating your BuildBuddy requests is with certificates. Your BuildBuddy certificates can be used by anyone in your organization, as it ties builds to your org - not your individual user.

You can download these certificates in your [setup instructions](https://app.buildbuddy.io/docs/setup/) once you've [created an account](http://app.buildbuddy.io/) and logged in. You'll first need to select `Certificate` as your auth option, then click `Download buildbuddy-cert.pem` and `Download buildbuddy-key.pem`.

Once you've downloaded your cert and key files - you can place them in a location that's only accessible to members of your organization and/or your CI machines.

You can then add the following lines to your `.bazelrc`:

```
build --tls_client_certificate=/path/to/your/buildbuddy-cert.pem
build --tls_client_key=/path/to/your/buildbuddy-key.pem
```

Make sure to update the paths to point to the location in which you've placed the files. If placing them in your workspace root, you can simply do:

```
build --tls_client_certificate=buildbuddy-cert.pem
build --tls_client_key=buildbuddy-key.pem
```
