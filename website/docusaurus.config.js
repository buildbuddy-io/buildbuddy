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
      switchConfig: {
        darkIcon: " ",
        lightIcon: " ",
      },
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
            { label: "Remote Execution", href: "/remote-execution" },
            { label: "Remote Cache", href: "/remote-cache" },
            { label: "Workflows", href: "/workflows" },
            { label: "BuildBuddy CLI", href: "/cli" },
            { label: "Plugin Library", href: "/plugins" },
            // {label: "Cloud", href: "/cloud"},
            // {label: "Open Source", href: "/open-source"},
            // {label: "Enterprise", href: "/enterpise"},
          ],
        },
        {
          href: "/pricing",
          target: "_self",
          label: "Pricing",
          position: "left",
        },
        {
          href: "https://github.com/buildbuddy-io/buildbuddy",
          target: "_blank",
          label: "GitHub",
          position: "left",
        },
        {
          href: "/blog/",
          activeBasePath: "/blog/",
          label: "Blog",
          position: "left",
        },
        {
          href: "/docs/introduction/",
          activeBasePath: "/docs/",
          label: "Docs",
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
          label: "Get Started",
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
              label: "Get Started",
              href: "https://app.buildbuddy.io",
              target: "_self",
            },
            {
              label: "Documentation",
              href: "/docs/introduction/",
              target: "_self",
            },
            {
              label: "Pricing",
              href: "/pricing",
              target: "_self",
            },
            {
              label: "Login",
              href: "https://app.buildbuddy.io/",
              target: "_self",
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
          title: "Company",
          items: [
            {
              label: "Blog",
              href: "/blog/",
              target: "_self",
            },
            {
              label: "Report an Issue",
              href: "https://github.com/buildbuddy-io/buildbuddy/issues/new",
            },
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
          ],
        },
        {
          title: "Security",
          items: [
            {
              label: "Security Overview",
              href: "/security",
              target: "_self",
            },
            {
              label: "Security Updates",
              href: "/security-updates",
              target: "_self",
            },
            {
              label: "Report a Vulnerability",
              href: "/security-vulnerability-report",
              target: "_self",
            },
          ],
        },
        {
          title: "Connect",
          items: [
            {
              label: "Slack",
              href: "http://slack.buildbuddy.io/",
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
  ],
};
