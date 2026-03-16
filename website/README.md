# Website

This website is built using [Docusaurus](https://docusaurus.io/), a modern static website generator.

## Installation

```bash
npm ci
```

## Local Development

```bash
npm run start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

## Build

```bash
npm run build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

## Deployment

GitHub Pages deployment is automated via `.github/workflows/website.yml`.

- Pushes to `main` automatically run a build job and publish to Pages.
- The workflow can also be triggered manually from GitHub Actions (`workflow_dispatch`).

To verify a production bundle locally:

```bash
npm run build
npm run serve
```
