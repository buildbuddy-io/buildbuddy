import React from "react";
import common from "../../css/common.module.css";
import styles from "./oss.module.css";
import Image from "@theme/IdealImage";

const repos = [
  {
    name: "elastic/kibana",
    description: "Kibana is a browser-based analytics and search dashboard for Elasticsearch.",
  },
  { name: "rabbitmq/rabbitmq-server", description: "RabbitMQ is a feature rich, multi-protocol messaging broker." },
  {
    name: "lewish/asciiflow",
    description: "ASCIIFlow is a client-side only web based application for drawing ASCII diagrams.",
  },
  { name: "pixie-io/pixie", description: "Instant Kubernetes-Native Application Observability." },
  {
    name: "formatjs/formatjs",
    description: "FormatJS is a modular collection of JavaScript libraries for internationalization.",
  },
  { name: "wix/greyhound", description: "A high-level Scala/Java SDK for Apache Kafka." },
  { name: "brendanhay/amazonka", description: "A comprehensive Amazon Web Services SDK for Haskell." },
  { name: "codeintelligencetesting/jazzer", description: "Coverage-guided, in-process fuzzing for the JVM." },
  { name: "tweag/rules_haskell", description: "Haskell rules for Bazel." },
  { name: "tweag/rules_nixpkgs", description: "Rules for importing Nixpkgs packages into Bazel." },
  {
    name: "thundergolfer/example-bazel-monorepo",
    description: "Example Bazel-ified monorepo, supporting Golang, Java, Python, Scala, and Typescript.",
  },
  { name: "protoconf/protoconf", description: "Configuration as Code framework based on protobuf and Starlark." },
  { name: "grailbio/rules_r", description: "R rules for Bazel." },
  {
    name: "square/bazel_maven_repository",
    description:
      "A Bazel ruleset creating a more idiomatic Bazel representation of a maven repo using a pinned list of artifacts.",
  },
  { name: "hdl/bazel_rules_hdl", description: "Hardware Description Language rules for Bazel." },
  { name: "apple-cross-toolchain/rules_applecross", description: "Bazel Apple toolchain for non-Apple platforms." },
  { name: "wix/exodus", description: "A tool that helps easily migrate your JVM code from Maven to Bazel." },
  { name: "curtismuntz/witness", description: "An API controllable webcam project." },
  { name: "caperai/branchpoke", description: "Poke developers to remind them to clean up their open branches." },
  { name: "dvulpe/bazel-terraform-rules", description: "Bazel rules for managing Terraform modules." },
  {
    name: "nitnelave/lru_cache",
    description: "A fast, header-only, generic C++ 17 LRU cache library, with customizable backend.",
  },
  {
    name: "antmicro/distant-bes",
    description: "A library for injecting build results into services implementing Bazel's Build Event Protocol.",
  },
  { name: "samhowes/rules_msbuild", description: "Build .csproj files with Bazel." },
  { name: "samhowes/rules_tsql", description: "TSQL Rules for Bazel." },
];

const globalSlash = new RegExp("/", "g");

function Component(props) {
  return (
    <div className={styles.repos}>
      {repos.slice(0, props.length).map((repo) => (
        <div className={styles.repo} key={repo.name}>
          <Image
            alt={`${repo.name} Github Repository Powered By BuildBuddy`}
            className={styles.repoImage}
            img={require(`../../../static/img/oss/${repo.name.replace(globalSlash, "_")}.png`)}
            shouldAutoDownload={() => true}
            threshold={10000}
          />
          <div className={styles.repoTitle}>{repo.name}</div>
          <div className={styles.repoDescription}>{repo.description}</div>
          <a href={`https://github.com/${repo.name}`} target="_blank" className={styles.link}>
            Learn more
          </a>
          <div className={common.spacer}></div>
        </div>
      ))}
    </div>
  );
}

export default Component;
