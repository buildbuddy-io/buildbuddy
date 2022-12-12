# buildbuddy.io marketing website

## Development

To run the marketing website locally for development, run:

```bash
bazel run website:start
```

View the marketing site at http://localhost:3000/

This page will live reload as you make and save changes. Perfect for iterating quickly.

# Production preview

To preview a static production build locally, run:

```bash
bazel run website:serve
```

View the marketing site at http://localhost:3000/

Instead of using the live reloading dev server, this will serve the website out of the production static bundle. This means you'll get optimized images, minified js/css, and all of the production-ready bells and whistles. You'll need to rebuild this target for it to pick up any changes.

# Bundle

To generate a static `.tar` archive of the marketing website.

```bash
bazel build website
```

## Deployment

To deploy the marketing website, just commit your changes and make sure the Deploy Website action succeeds.
