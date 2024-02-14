const fs = require("fs");

// Keep these in sync with /tsconfig.json
const configDirs = [
  "bazel-out/k8-opt/bin",
  "bazel-out/k8-fastbuild/bin",
  "bazel-out/linux_x86_64-fastbuild/bin",
  "bazel-out/linux_x86_64-opt/bin",
  "bazel-out/darwin-opt/bin",
  "bazel-out/darwin-fastbuild/bin",
  "bazel-out/darwin_x86_64-opt/bin",
  "bazel-out/darwin_x86_64-fastbuild/bin",
  "bazel-out/darwin_arm64-opt/bin",
  "bazel-out/darwin_arm64-fastbuild/bin",
  "bazel-out/macos_x86_64-opt/bin",
  "bazel-out/macos_x86_64-fastbuild/bin",
  "bazel-out/macos_arm64-opt/bin",
  "bazel-out/macos_arm64-fastbuild/bin",
  "bazel-out/local_config_platform-opt/bin",
  "bazel-out/local_config_platform-fastbuild/bin",
];

/**
 * Webpack plugin that resolves "bazel-bin" references to the bazel
 * config-specific "bazel-out/.../bin" directory.
 */
class BazelBinResolverPlugin {
  apply(resolver) {
    const target = resolver.ensureHook("resolve");

    resolver.getHook("resolve").tapAsync("BazelBinResolverPlugin", (request, resolveContext, callback) => {
      if (request?.request?.includes("/bazel-bin/")) {
        // For now just try all supported config dirs and see if the file exists there.
        for (const configDir of configDirs) {
          // Note: ROOTDIR (execution root dir) is set by the yarn() rule in
          // //rules/yarn:index.bzl
          const binPath = request.request.replace(/.*?\/bazel-bin\//, process.env["ROOTDIR"] + "/" + configDir + "/");
          let exists = false;
          try {
            fs.statSync(binPath);
            exists = true;
          } catch (e) {}
          if (!exists) {
            continue;
          }
          resolver.doResolve(target, { ...request, request: binPath }, null, resolveContext, callback);
          return;
        }
      }
      callback();
    });
  }
}

const bazelBinPlugin = function (context, options) {
  return {
    name: "docusaurus-bazel-bin-resolver-plugin",

    configureWebpack() {
      return {
        resolve: { plugins: [new BazelBinResolverPlugin()] },
      };
    },
  };
};

module.exports = {
  title: "BuildBuddy",
  tagline:
    "BuildBuddy provides enterprise features for Bazel — the open source build system that allows you to build and test software 10x faster.",
  url: "https://www.buildbuddy.io",
  baseUrl: "/",
  favicon: "img/favicon_black.svg",
  organizationName: "buildbuddy-io",
  projectName: "buildbuddy",
  themeConfig: {
    metadata: [
      {
        name: "description",
        content:
          "BuildBuddy provides enterprise features for Bazel — the open source build system that allows you to build and test software 10x faster.",
      },
      {
        name: "og:description",
        content:
          "BuildBuddy provides enterprise features for Bazel — the open source build system that allows you to build and test software 10x faster.",
      },
    ],
    colorMode: {
      disableSwitch: true,
    },
    prism: {
      additionalLanguages: ["promql", "protobuf"],
    },
    image: "img/preview.png",
    navbar: {
      logo: {
        alt: "BuildBuddy Logo",
        src: "img/logo.svg",
        srcDark: "img/logo_white.svg",
        target: "_self",
        href: "/",
        width: "191px",
        height: "32px",
      },
      items: [
        {
          label: "Features",
          position: "left",
          type: "dropdown",
          to: "features",
          items: [
            { label: "Build & Test UI", href: "/ui" },
            {
              label: "Remote Execution",
              href: "/remote-execution",
            },
            { label: "Remote Cache", href: "/remote-cache" },
            { label: "Workflows", href: "/workflows" },
            { label: "BuildBuddy CLI", href: "/cli" },
          ],
        },
        // TODO(siggisim): Make pages for these
        // {
        //   href: "/solutions",
        //   target: "_self",
        //   label: "Solutions",
        //   position: "left",
        //   type: "dropdown",
        //   items: [
        //     { label: "Cloud", href: "/cloud" },
        //     { label: "Open Source", href: "/open-source" },
        //     { label: "Enterprise", href: "/enterprise" },
        //     { label: "iOS", href: "/cli" },
        //   ],
        // },
        {
          label: "Resources",
          position: "left",
          type: "dropdown",
          items: [
            { label: "Blog", href: "/blog" },
            { label: "GitHub", href: "https://github.com/buildbuddy-io/buildbuddy" },
            { label: "Community", href: "http://community.buildbuddy.io/" },
            { label: "Security", href: "/security" },
            { label: "Plugins", href: "/plugins" },
            { label: "Team", href: "/team" },
            // TODO(siggisim): Make pages for these.
            // { label: "Guides", href: "/guides" },
            // { label: "Module Registry", href: "/module-registry" },
            // { label: "Templates", href: "/templates" },
            // { label: "Bazel", href: "/bazel/" },
            // { label: "Help Center", href: "/help/" },
            // { label: "Customers", href: "/customers/" },
          ],
        },
        {
          href: "/docs/introduction/",
          activeBasePath: "/docs/",
          label: "Docs",
          position: "left",
        },
        {
          href: "/pricing",
          target: "_self",
          label: "Pricing",
          position: "left",
        },
        {
          href: "/contact",
          target: "_self",
          label: "Contact",
          position: "right",
        },
        {
          href: "https://app.buildbuddy.io/",
          target: "_self",
          label: "Login",
          position: "right",
        },
        {
          href: "https://app.buildbuddy.io/",
          target: "_self",
          label: "Sign up",
          position: "right",
          className: "sign-up",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          items: [
            {
              html: `<a href="/"><img alt="BuildBuddy Logo" src="/img/logo_white.svg" width="281px" height="32px" class="footer-logo" /></a>`,
            },
          ],
        },
        {
          title: "Product",
          items: [
            {
              label: "Build & Test UI",
              href: "/ui",
              target: "_self",
            },
            {
              label: "Remote Execution",
              href: "/remote-execution",
              target: "_self",
            },
            {
              label: "Remote Cache",
              href: "/remote-cache",
              target: "_self",
            },
            {
              label: "Workflows",
              href: "/workflows",
              target: "_self",
            },
            {
              label: "CLI",
              href: "/cli",
              target: "_self",
            },
            {
              label: "Get Started",
              href: "https://app.buildbuddy.io",
              target: "_self",
            },
            {
              label: "Login",
              href: "https://app.buildbuddy.io/",
              target: "_self",
            },
          ],
        },
        {
          title: "Resources",
          items: [
            {
              label: "Docs",
              href: "/docs/introduction/",
              target: "_self",
            },
            {
              label: "Pricing",
              href: "/pricing",
              target: "_self",
            },
            {
              label: "Blog",
              href: "/blog/",
              target: "_self",
            },
            {
              label: "Security",
              href: "/security",
              target: "_self",
            },
            {
              label: "Plugins",
              href: "/plugins",
              target: "_self",
            },
            {
              label: "API",
              href: "/docs/enterprise-api",
              target: "_self",
            },
          ],
        },
        {
          title: "Company",
          items: [
            {
              label: "Contact Us",
              href: "/contact",
              target: "_self",
            },
            {
              label: "Team",
              href: "/team",
              target: "_self",
            },
            {
              label: "Careers",
              href: "/careers",
              target: "_self",
            },
            {
              label: "Report an Issue",
              href: "https://github.com/buildbuddy-io/buildbuddy/issues/new",
            },
            {
              label: "Privacy Policy",
              href: "/privacy",
              target: "_self",
            },
            {
              label: "Terms of Service",
              href: "/terms",
              target: "_self",
            },
          ],
        },
        {
          title: "Connect",
          items: [
            {
              label: "Slack",
              href: "http://community.buildbuddy.io/",
            },
            {
              label: "Twitter",
              href: "https://twitter.com/buildbuddy_io",
            },
            {
              label: "LinkedIn",
              href: "http://linkedin.com/company/buildbuddy",
            },
            {
              label: "GitHub",
              href: "https://github.com/buildbuddy-io",
            },
          ],
        },
      ],
      copyright: `© ${new Date().getFullYear()} Iteration, Inc.`,
    },
  },
  presets: [
    [
      "@docusaurus/preset-classic",
      {
        docs: {
          sidebarCollapsed: false,
          path: "../docs",
          sidebarPath: require.resolve("./sidebars.js"),
          editUrl: "https://github.com/buildbuddy-io/buildbuddy/edit/master/docs/",
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
        },
        blog: {
          path: "blog",
          showReadingTime: true,
          blogSidebarCount: 10,
          editUrl: "https://github.com/buildbuddy-io/buildbuddy/edit/master/website/",
          blogPostComponent: "../theme/BlogPostPage",
          blogListComponent: "../theme/BlogListPage",
          blogTagsListComponent: "../theme/BlogTagsListPage",
          blogTagsPostsComponent: "../theme/BlogTagsPostsPage",
        },
        googleAnalytics: {
          trackingID: "UA-156160991-1",
        },
        theme: {
          customCss: [
            require.resolve("./src/css/footer.css"),
            require.resolve("./src/css/general.css"),
            require.resolve("./src/css/markdown.css"),
            require.resolve("./src/css/nav.css"),
          ],
        },
      },
    ],
  ],
  scripts: [],
  plugins: [
    [
      "@docusaurus/plugin-ideal-image",
      {
        sizes: [1600],
        disableInDev: false,
      },
    ],
    [
      require.resolve("@cmfcmf/docusaurus-search-local"),
      {
        indexDocs: true,
        indexBlog: true,
      },
    ],
    [
      "@docusaurus/plugin-client-redirects",
      {
        redirects: [
          {
            to: "/docs/introduction",
            from: ["/faq"],
          },
          {
            to: "/docs/introduction",
            from: ["/docs"],
          },
        ],
      },
    ],
    bazelBinPlugin,
  ],
};
