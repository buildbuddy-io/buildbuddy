# buildbuddy.io marketing website

## Development

To run the marketing website locally, run:

```bash
bazel run website:start
```

View the marketing site at http://localhost:3000/

This page will live reload as you make and save changes.

# Production preview

To preview a static production build locally, run:

```bash
bazel run website:serve
```

View the marketing site at http://localhost:3000/

You'll need to rebuild this target for it to pick up any changes.

# Bundle

To generate a static `.tar` archive of the marketing website.

```bash
bazel build website
```

## Deployment

To deploy the marketing website, just commit your changes.
