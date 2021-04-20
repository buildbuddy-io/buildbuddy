module.exports = {
  title: 'BuildBuddy',
  tagline: 'BuildBuddy provides enterprise features for Bazel — the open source build system that allows you to build and test software 10x faster.',
  url: 'https://buildbuddy.io',
  baseUrl: '/',
  favicon: 'img/favicon_black.svg',
  organizationName: 'buildbuddy-io',
  projectName: 'buildbuddy',
  themeConfig: {
    colorMode: {
      switchConfig: {
        darkIcon: ' ',
        lightIcon: ' ',
      },
    },
    prism: {
      additionalLanguages: ['promql'],
    },
    navbar: {
      logo: {
        alt: 'BuildBuddy Logo',
        src: 'img/logo.svg',
        srcDark: 'img/logo_white.svg',
        target: '_self',
        href: 'https://www.buildbuddy.io'
      },
      items: [
        {
          to: '/docs/introduction',
          activeBasePath: '/docs/',
          label: 'Docs',
          position: 'left',
        },
        {
          href: 'https://buildbuddy.io/faq',
          target: '_self',
          label: 'FAQs',
          position: 'left',
        },
        {
          href: 'https://github.com/buildbuddy-io/buildbuddy',
          target: '_self',
          label: 'GitHub',
          position: 'left',
        },
        {
          href: 'https://buildbuddy.io/pricing',
          target: '_self',
          label: 'Pricing',
          position: 'left',
        },
        {
          href: 'https://app.buildbuddy.io/',
          target: '_self',
          label: 'Login',
          position: 'left',
        },

        {
          href: 'https://app.buildbuddy.io/',
          target: '_self',
          label: 'Sign Up',
          position: 'right',
          class: 'sign-up'
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          items: [
            {
              html: `<a href="https://www.buildbuddy.io/"><img src="/img/logo_white.svg" class="footer-logo" /></a>`,
            }

          ],
        },
        {
          title: 'Product',
          items: [
            {
              label: 'Get Started',
              href: 'https://app.buildbuddy.io/',
              target: '_self',
            },
            {
              label: 'Documentation',
              href: 'https://www.buildbuddy.io/docs/',
              target: '_self',
            },
            {
              label: 'Pricing',
              href: 'https://www.buildbuddy.io/pricing',
              target: '_self',
            },
            {
              label: 'Login',
              href: 'https://app.buildbuddy.io/',
              target: '_self',
            },
            {
              label: 'Privacy Policy',
              href: 'https://www.buildbuddy.io/privacy',
              target: '_self',
            },
            {
              label: 'Terms of Service',
              href: 'https://www.buildbuddy.io/terms',
              target: '_self',
            },
          ],
        },
        {
          title: 'Company',
          items: [
            {
              label: 'Blog',
              href: 'https://www.buildbuddy.io/blog',
              target: '_self',
            },
            {
              label: 'Report an Issue',
              href: 'https://github.com/buildbuddy-io/buildbuddy/issues/new',
            },
            {
              label: 'Contact Us',
              href: 'https://www.buildbuddy.io/contact',
              target: '_self',
            },
            {
              label: 'Our Team',
              href: 'https://www.buildbuddy.io/team',
              target: '_self',
            },
            {
              label: 'Careers',
              href: 'https://www.buildbuddy.io/careers',
              target: '_self',
            },
          ],
        },
        {
          title: 'Security',
          items: [
            {
              label: 'Security Overview',
              href: 'https://www.buildbuddy.io/security',
              target: '_self',
            },
            {
              label: 'Security Updates',
              href: 'https://www.buildbuddy.io/security-updates',
              target: '_self',
            },
            {
              label: 'Report a Vulnerability',
              href: 'https://www.buildbuddy.io/security-vulnerability-report',
              target: '_self',
            },
          ],
        },
        {
          title: 'Connect',
          items: [
            {
              label: 'Slack',
              href: 'http://slack.buildbuddy.io/',
            },
            {
              label: 'Twitter',
              href: 'https://twitter.com/buildbuddy_io',
            },
            {
              label: 'LinkedIn',
              href: 'http://linkedin.com/company/buildbuddy',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/buildbuddy-io',
            },
          ],
        },
        
      ],
      copyright: `© ${new Date().getFullYear()} Iteration, Inc.`,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          path: '../docs',
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/buildbuddy-io/buildbuddy/edit/master/docs/',
        },
        blog: {
          path: 'blog',
          showReadingTime: true,
          blogSidebarCount: 5,
          editUrl: 'https://github.com/buildbuddy-io/buildbuddy/edit/master/blog/',
        },
        theme: {
          customCss: [
            require.resolve('./src/css/footer.css'),
            require.resolve('./src/css/general.css'),
            require.resolve('./src/css/markdown.css'),
            require.resolve('./src/css/navbar.css'),
          ],
        },
      },
    ],
  ],
  plugins: [
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: [
          {
            to: process.env.BLOG ? '/blog' : '/docs/introduction',
            from: ['/'],
          },
          {
            to: '/docs/introduction',
            from: ['/docs'],
          },
        ],
      },
    ],
  ],
};
