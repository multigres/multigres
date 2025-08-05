import { themes as prismThemes } from 'prism-react-renderer'
import type { Config } from '@docusaurus/types'
import type * as Preset from '@docusaurus/preset-classic'

const config: Config = {
  title: 'Multigres',
  tagline: 'Multigres: Vitess for Postgres',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://multigres.com',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'multigres', // Usually your GitHub org/user name.
  projectName: 'multigres.com', // Usually your repo name.
  deploymentBranch: 'gh-pages',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/multigres/multigres/tree/main/site/',
          sidebarCollapsed: false,
        },
        blog: {
          showReadingTime: true,
          editUrl:
            'https://github.com/multigres/multigres/tree/main/site/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    // Replace with your project's social card
    image: 'img/og-image.png',
    defaultMode: 'dark',
    colorMode: {
      defaultMode: 'dark',
      disableSwitch: true,
      respectPrefersColorScheme: false,
    },
    navbar: {
      // title: "Multigres",
      logo: {
        alt: 'Multigres',
        src: 'img/logo-horizontal.png',
      },
      items: [
        { to: '/docs', label: 'Docs', position: 'right' },
        { to: '/blog', label: 'Blog', position: 'right' },
        {
          href: 'https://github.com/multigres/multigres',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    stylesheets: [
      {
        rel: 'preconnect',
        href: 'https://fonts.googleapis.com',
      },
      {
        rel: 'preconnect',
        href: 'https://fonts.gstatic.com',
        crossorigin: 'anonymous',
      },
      {
        href: 'https://fonts.googleapis.com/css2?family=Reddit+Mono:wght@200..900&display=swap',
        type: 'text/css',
        crossorigin: 'anonymous',
      },
    ],

    footer: {
      style: 'dark',
      links: [
        {
          title: 'Site',
          items: [
            {
              label: 'Docs',
              to: '/docs',
            },
            {
              label: 'Blog',
              to: '/blog',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/multigres/multigres',
            },
            {
              label: 'Twitter',
              href: 'https://twitter.com/multigres',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Supabase Inc`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
    plugins: [
      [
        '@docusaurus/plugin-content-docs',
        {
          id: 'docs-intro',
          path: 'docs',
          routeBasePath: '/docs',
          configureWebpack: () => ({
            resolve: {
              symlinks: true,
            },
          }),
        },
      ],
    ],
  } satisfies Preset.ThemeConfig,
}

export default config
