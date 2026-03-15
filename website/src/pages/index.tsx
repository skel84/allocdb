import clsx from "clsx";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Layout from "@theme/Layout";
import Heading from "@theme/Heading";

import styles from "./index.module.css";

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx("hero hero--primary", styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/product"
          >
            Read the Docs
          </Link>
          <Link
            className="button button--secondary button--outline button--lg"
            style={{ marginLeft: "10px" }}
            to="/docs/architecture"
          >
            View Architecture
          </Link>
          <Link
            className="button button--secondary button--outline button--lg"
            style={{ marginLeft: "10px" }}
            to="/blog"
          >
            Read Blog
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): JSX.Element {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`Welcome to ${siteConfig.title}`}
      description="A deterministic resource-allocation database for ticketing, inventory, and more."
    >
      <HomepageHeader />
      <main>
        <div className="container" style={{ padding: "4rem 0" }}>
          <div className="row">
            <div className="col col--8 col--offset-2">
              <h2>The Problem</h2>
              <p>
                Modern systems repeatedly rebuild reservation logic on top of
                generic databases, caches, queues, and cleanup workers. Typical
                stacks rely on PostgreSQL row locking, Redis TTL keys, ad-hoc
                queues, and background cleanup workers.
              </p>
              <p>
                These systems frequently fail by causing double allocations,
                ghost reservations, race conditions, retry storms, and fragile
                crash recoveries.
              </p>
              <h2>The Solution</h2>
              <p>
                <strong>AllocDB</strong> exists to make "one resource, one
                winner" a first-class primitive. It exists to remove these
                failure modes from application code.
              </p>
              <ul>
                <li>
                  <strong>Determinism First:</strong> The same snapshot and WAL
                  must always produce the same state.
                </li>
                <li>
                  <strong>Boundedness First:</strong> Every queue, table, batch,
                  payload, and retention window has an explicit limit.
                </li>
                <li>
                  <strong>Built in Rust:</strong> Trusted-core code targets
                  allocation-free steady-state execution after startup.
                </li>
              </ul>
            </div>
          </div>
        </div>
      </main>
    </Layout>
  );
}
