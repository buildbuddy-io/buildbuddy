const baseUrl = "/";
module.exports = {
  title: 'BuildBuddy',
  tagline: 'BuildBuddy provides enterprise features for Bazel — the open source build system that allows you to build and test software 10x faster.',
  url: 'https://buildbuddy.io',
  baseUrl: baseUrl,
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
          label: 'FAQs',
          position: 'left',
        },
        {
          href: 'https://github.com/buildbuddy-io/buildbuddy',
          label: 'GitHub',
          position: 'left',
        },
        {
          href: 'https://buildbuddy.io/pricing',
          label: 'Pricing',
          position: 'left',
        },
        {
          href: 'https://app.buildbuddy.io/',
          label: 'Login',
          position: 'left',
        },

        {
          href: 'https://app.buildbuddy.io/',
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
              html: `<a href="/"><img src="${baseUrl}img/logo_white.svg" class="footer-logo" /></a>`,
            }

          ],
        },
        {
          title: 'Product',
          items: [
            {
              label: 'Get Started',
              href: 'https://app.buildbuddy.io/',
            },
            {
              label: 'Documentation',
              href: 'https://www.buildbuddy.io/docs/',
            },
            {
              label: 'Pricing',
              href: 'https://www.buildbuddy.io/pricing',
            },
            {
              label: 'Login',
              href: 'https://app.buildbuddy.io/',
            },
            {
              label: 'Privacy Policy',
              href: 'https://www.buildbuddy.io/privacy',
            },
            {
              label: 'Terms of Service',
              href: 'https://www.buildbuddy.io/terms',
            },
          ],
        },
        {
          title: 'Company',
          items: [
            {
              label: 'Blog',
              href: 'https://www.buildbuddy.io/blog',
            },
            {
              label: 'Report an Issue',
              href: 'https://github.com/buildbuddy-io/buildbuddy/issues/new',
            },
            {
              label: 'Contact Us',
              href: 'https://www.buildbuddy.io/contact',
            },
            {
              label: 'Our Team',
              href: 'https://www.buildbuddy.io/team',
            },
            {
              label: 'Careers',
              href: 'https://www.buildbuddy.io/careers',
            },
          ],
        },
        {
          title: 'Security',
          items: [
            {
              label: 'Security Overview',
              href: 'https://www.buildbuddy.io/security',
            },
            {
              label: 'Security Updates',
              href: 'https://www.buildbuddy.io/security-updates',
            },
            {
              label: 'Report a Vulnerability',
              href: 'https://www.buildbuddy.io/security-vulnerability-report',
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
          editUrl:
          'https://github.com/buildbuddy-io/buildbuddy/edit/master/',
        },
        blog: {
          showReadingTime: true,
          editUrl:
            'https://github.com/buildbuddy-io/buildbuddy/edit/master/',
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
            to: '/docs/introduction',
            from: ['/', '/docs'],
          },
        ],
      },
    ],
  ],
};
