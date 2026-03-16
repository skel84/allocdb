import type { ReactNode } from "react";
import clsx from "clsx";
import Heading from "@theme/Heading";
import styles from "./styles.module.css";

type FeatureItem = {
  title: string;
  Svg: React.ComponentType<React.ComponentProps<"svg">>;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: "Deterministic Allocation",
    Svg: require("@site/static/img/logo.svg").default,
    description: (
      <>
        Every reservation state transition is deterministic so concurrent
        workers and retries converge on a single authoritative outcome.
      </>
    ),
  },
  {
    title: "Built-in Boundedness",
    Svg: require("@site/static/img/logo.svg").default,
    description: (
      <>
        Safety, queue depth, and retention are bounded by design, preventing
        runaway growth under adversarial retry and restart conditions.
      </>
    ),
  },
  {
    title: "Observability First",
    Svg: require("@site/static/img/logo.svg").default,
    description: (
      <>
        Expose allocator behavior through tracing and metrics so recovery paths,
        queue pressure, and reservation lifecycle are easy to reason about in
        operations.
      </>
    ),
  },
];

function Feature({ title, Svg, description }: FeatureItem) {
  return (
    <div className={clsx("col col--4")}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
