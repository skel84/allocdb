import { themes as prismThemes } from "prism-react-renderer";
import type { Config } from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";

const config: Config = {
  title: "AllocDB",
  tagline: "A deterministic resource-allocation database",
  favicon: "img/favicon.ico",

  url: "https://skel84.github.io",
  baseUrl: "/allocdb/",

  organizationName: "skel84",
  projectName: "allocdb",

  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "throw",

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      {
        docs: {
          path: "../docs",
          sidebarPath: "./sidebars.ts",
          routeBasePath: "docs",
        },
        blog: {
          showReadingTime: true,
        },
        theme: {
          customCss: "./src/css/custom.css",
        },
      } satisfies Preset.Options,
    ],
  ],
  scripts: [
    {
      src: "https://plausible.rbl.lol/js/script.js",
      defer: true,
      "data-domain": "skel84.github.io",
    } as { src: string; defer: boolean; "data-domain": string },
  ],

  themeConfig: {
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: "AllocDB",
      items: [
        {
          type: "docSidebar",
          sidebarId: "tutorialSidebar",
          position: "left",
          label: "Documentation",
        },
        {
          to: "/blog",
          label: "Blog",
          position: "left",
        },
        {
          href: "https://github.com/skel84/allocdb",
          label: "GitHub",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Docs",
          items: [
            {
              label: "Introduction",
              to: "/docs/product",
            },
            {
              label: "Architecture",
              to: "/docs/architecture",
            },
          ],
        },
        {
          title: "Community",
          items: [
            {
              label: "Blog",
              to: "/blog",
            },
            {
              label: "GitHub",
              href: "https://github.com/skel84/allocdb",
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} AllocDB Contributors. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ["rust", "toml"],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
