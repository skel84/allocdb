import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebars: SidebarsConfig = {
  tutorialSidebar: [
    {
      type: "category",
      label: "Overview",
      collapsed: false,
      items: ["product", "principles", "semantics", "api"],
    },
    {
      type: "category",
      label: "Engineering & Architecture",
      collapsed: false,
      items: [
        "architecture",
        "operator-runbook",
        "fault-model",
        "storage",
        "implementation",
        "testing",
      ],
    },
    {
      type: "category",
      label: "Research & Status",
      collapsed: false,
      items: ["jepsen-testing", "replication"],
    },
  ],
};

export default sidebars;
