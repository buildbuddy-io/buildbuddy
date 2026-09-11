import Image from "@theme/IdealImage";
import React from "react";
import styles from "./oss.module.css";

type Repository = {
  name: string;
  description: string;
  evidenceUrl?: string;
  image: unknown;
};

const repos: Repository[] = [
  {
    name: "openai/codex",
    description: "A lightweight coding agent that runs in your terminal.",
    evidenceUrl:
      "https://github.com/openai/codex/blob/0b94751cc463d02dec397c4c4dbb77fd9b93d94d/.github/workflows/bazel.yml",
    image: require("../../../static/img/oss/openai_codex.png"),
  },
  {
    name: "google/heir",
    description: "An MLIR-based compiler toolchain for fully homomorphic encryption.",
    evidenceUrl:
      "https://github.com/google/heir/blob/ae757edc0d670abee3cb6aa13c5cfd584e5934db/.github/workflows/build_and_test.yml",
    image: require("../../../static/img/oss/google_heir.png"),
  },
  {
    name: "modular/modular",
    description: "The Modular Platform, including MAX and the Mojo programming language.",
    evidenceUrl:
      "https://github.com/modular/modular/blob/1245611c0c6320f30e6459ce70d1fc7b668c026f/.bazelrc",
    image: require("../../../static/img/oss/modular_modular.png"),
  },
  {
    name: "apache/rocketmq",
    description: "A cloud-native messaging and streaming platform for event-driven applications.",
    evidenceUrl:
      "https://github.com/apache/rocketmq/blob/484b7b812c365c28ace88da72aa41bc30e010b10/.bazelrc",
    image: require("../../../static/img/oss/apache_rocketmq.png"),
  },
  {
    name: "MobileNativeFoundation/rules_xcodeproj",
    description: "Bazel rules for generating Xcode projects.",
    evidenceUrl:
      "https://github.com/MobileNativeFoundation/rules_xcodeproj/blob/f6a54168d19f917d0630e7d4efa8f2f9fa0ddc36/shared.bazelrc",
    image: require("../../../static/img/oss/MobileNativeFoundation_rules_xcodeproj.png"),
  },
  {
    name: "hermeticbuild/hermetic-llvm",
    description: "A zero-sysroot, hermetic C and C++ cross-compilation toolchain based on LLVM.",
    evidenceUrl:
      "https://github.com/hermeticbuild/hermetic-llvm/blob/6314688712edf3a95f78642d80393868256b4ef2/.github/workflows/ci.yaml",
    image: require("../../../static/img/oss/hermeticbuild_hermetic-llvm.png"),
  },
  {
    name: "formatjs/formatjs",
    description: "A monorepo of JavaScript internationalization libraries, including React Intl.",
    evidenceUrl:
      "https://github.com/formatjs/formatjs/blob/33decfcf838e98655ff066adf1d1bd070d438742/.bazelrc",
    image: require("../../../static/img/oss/formatjs_formatjs.png"),
  },
  {
    name: "pixie-io/pixie",
    description: "An open-source observability tool for Kubernetes applications.",
    evidenceUrl:
      "https://github.com/pixie-io/pixie/blob/830ff2ad3d5b6d4ff5a975ac5b21d43d42ce4bef/.bazelrc",
    image: require("../../../static/img/oss/pixie-io_pixie.png"),
  },
  {
    name: "lewish/asciiflow",
    description: "A client-side web application for drawing ASCII diagrams.",
    evidenceUrl:
      "https://github.com/lewish/asciiflow/blob/758bcb00fde489542baebcd0cabd2a4af1e586fe/.bazelrc",
    image: require("../../../static/img/oss/lewish_asciiflow.png"),
  },
  {
    name: "aya-rs/aya",
    description: "An eBPF library for Rust focused on developer experience and operability.",
    evidenceUrl:
      "https://github.com/aya-rs/aya/blob/f69d62a22be71c376b5e0fad2d95adbdf46d9800/.github/workflows/ci.yml",
    image: require("../../../static/img/oss/aya-rs_aya.png"),
  },
  {
    name: "zml/zml",
    description: "An AI inference stack built with Zig, MLIR, OpenXLA, and Bazel.",
    evidenceUrl:
      "https://github.com/zml/zml/blob/1b6a3cb3951c0a9b0a75525d3d9d7257e3f6ab0a/.github/workflows/ci.yaml",
    image: require("../../../static/img/oss/zml_zml.png"),
  },
  {
    name: "OffchainLabs/prysm",
    description: "A Go implementation of Ethereum proof of stake.",
    evidenceUrl:
      "https://github.com/OffchainLabs/prysm/blob/a501af026e6651f7aebf2c2169517328e67cd55a/.buildkite-bazelrc",
    image: require("../../../static/img/oss/OffchainLabs_prysm.png"),
  },
  {
    name: "dfinity/ic",
    description: "The Internet Computer blockchain client and replica software run by nodes.",
    evidenceUrl:
      "https://github.com/dfinity/ic/blob/6328c38fcd607f61ac5a6ab6cfb790b3c7c2f032/.github/actions/bazel/action.yaml",
    image: require("../../../static/img/oss/dfinity_ic.png"),
  },
  {
    name: "wpilibsuite/allwpilib",
    description: "The official repository of the WPILibJ and WPILibC robotics libraries.",
    evidenceUrl:
      "https://github.com/wpilibsuite/allwpilib/blob/b448d64f308293d4d8dcbc1401db6d96e520fd11/.github/workflows/bazel.yml",
    image: require("../../../static/img/oss/wpilibsuite_allwpilib.png"),
  },
  {
    name: "CodeIntelligenceTesting/jazzer",
    description: "Coverage-guided, in-process fuzzing for the JVM.",
    evidenceUrl:
      "https://github.com/CodeIntelligenceTesting/jazzer/blob/50a0e8f2c3aa0d28165b860e1c942383ba08d180/.github/workflows/run-all-tests-pr.yml",
    image: require("../../../static/img/oss/codeintelligencetesting_jazzer.png"),
  },
  {
    name: "GerritCodeReview/gerrit",
    description: "A web-based code review system for Git repositories.",
    evidenceUrl:
      "https://github.com/GerritCodeReview/gerrit/blob/82300fd56568aac49adbd81570f708c54230e6d0/tools/remote-bazelrc",
    image: require("../../../static/img/oss/GerritCodeReview_gerrit.png"),
  },
  {
    name: "BYVoid/OpenCC",
    description: "A library for converting text between Traditional and Simplified Chinese.",
    evidenceUrl:
      "https://github.com/BYVoid/OpenCC/blob/fa130de5740318ed9c7375e0db2ca25c1c33ca38/.github/workflows/bazel.yml",
    image: require("../../../static/img/oss/BYVoid_OpenCC.png"),
  },
  {
    name: "pachyderm/pachyderm",
    description: "A data pipeline platform with data versioning and lineage.",
    evidenceUrl:
      "https://github.com/pachyderm/pachyderm/blob/e237475e9910a2d6299d7d2c3d6fc3b9a8f28f0b/.bazelrc",
    image: require("../../../static/img/oss/pachyderm_pachyderm.png"),
  },
  {
    name: "wix-incubator/exodus",
    description: "A tool for migrating JVM codebases from Maven to Bazel.",
    evidenceUrl:
      "https://github.com/wix-incubator/exodus/blob/dfb0c9713b07a8b6a49b548b7b543021e748d80b/.bazelrc.remote",
    image: require("../../../static/img/oss/wix-incubator_exodus.png"),
  },
  {
    name: "tweag/rules_haskell",
    description: "Bazel rules for building Haskell projects.",
    evidenceUrl:
      "https://github.com/tweag/rules_haskell/blob/4d94849132c05b2dbc7ebc3b2b802884a0e6fd25/.bazelrc.common",
    image: require("../../../static/img/oss/tweag_rules_haskell.png"),
  },
  {
    name: "withered-magic/starpls",
    description: "A language server for Starlark, the configuration language used by Bazel and Buck2.",
    evidenceUrl:
      "https://github.com/withered-magic/starpls/blob/ac25eca3dbbed6347fbca5fbf14d3a027d46bcab/bazel/remote-cache.bazelrc",
    image: require("../../../static/img/oss/withered-magic_starpls.png"),
  },
  {
    name: "world-federation-of-advertisers/cross-media-measurement",
    description: "A privacy-centric system for cross-publisher, cross-media advertising measurement.",
    evidenceUrl:
      "https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/1ab1adc722d5cd9198015d342e3ee294ab8b6601/.bazelrc",
    image: require("../../../static/img/oss/world-federation-of-advertisers_cross-media-measurement.png"),
  },
  {
    name: "square/bazel_maven_repository",
    description: "Bazel rules for representing a Maven repository from a pinned artifact list.",
    evidenceUrl:
      "https://github.com/square/bazel_maven_repository/blob/8f21d5989b87e801da81d5a12c9adc244fdb3c28/travis.bazelrc",
    image: require("../../../static/img/oss/square_bazel_maven_repository.png"),
  },
  {
    name: "hermeticbuild/linux.bzl",
    description: "Bazel rules for configuring and building Linux kernels from source.",
    evidenceUrl:
      "https://github.com/hermeticbuild/linux.bzl/blob/62e587a893f2aaaa4444330796cc5b32fb17abc3/.bazelrc.shared",
    image: require("../../../static/img/oss/hermeticbuild_linux.bzl.png"),
  },
  {
    name: "cgrindel/rules_swift_package_manager",
    description: "Bazel rules and utilities for using Swift Package Manager dependencies.",
    evidenceUrl:
      "https://github.com/cgrindel/rules_swift_package_manager/blob/7bc5dcffbe4ee804e73582f90505d8245140fee7/shared.bazelrc",
    image: require("../../../static/img/oss/cgrindel_rules_swift_package_manager.png"),
  },
  {
    name: "cgrindel/bazel-starlib",
    description: "Reusable rules, macros, and APIs for Bazel repositories.",
    evidenceUrl:
      "https://github.com/cgrindel/bazel-starlib/blob/9dda462b8d3ed9abdd1242875e674ef0f9dd6a2d/shared.bazelrc",
    image: require("../../../static/img/oss/cgrindel_bazel-starlib.png"),
  },
  {
    name: "rue-language/rue",
    description: "A programming language aiming for an abstraction level between Rust and Go.",
    evidenceUrl:
      "https://github.com/rue-language/rue/blob/84320429fea20f05f3d35999506bc9b0afa895dd/.github/workflows/ci.yml",
    image: require("../../../static/img/oss/rue-language_rue.png"),
  },
  {
    name: "carverauto/serviceradar",
    description: "Open-source network management, monitoring, IT operations, and security analytics.",
    evidenceUrl:
      "https://github.com/carverauto/serviceradar/blob/63f0632b871cc2dc008081a6bf75e25aa10eb75f/.bazelrc",
    image: require("../../../static/img/oss/carverauto_serviceradar.png"),
  },
  {
    name: "mull-project/mull",
    description: "Mutation testing and fault injection for C and C++.",
    evidenceUrl:
      "https://github.com/mull-project/mull/blob/a83b055f77b3b9b9083a8e05cedec4ebaa22a521/.github/actions/setup-bazel-cache/action.yml",
    image: require("../../../static/img/oss/mull-project_mull.png"),
  },
  {
    name: "tweag/rules_nixpkgs",
    description: "Bazel rules for importing packages from Nixpkgs.",
    evidenceUrl:
      "https://github.com/tweag/rules_nixpkgs/blob/3308e812ac6e9782c97cbc430f860b4c9d94ad0a/.github/workflows/workflow.yaml",
    image: require("../../../static/img/oss/tweag_rules_nixpkgs.png"),
  },
  {
    name: "bazel-ios/rules_ios",
    description: "Bazel rules for building iOS applications and frameworks.",
    evidenceUrl:
      "https://github.com/bazel-ios/rules_ios/blob/0a0d0f886e90fe647047b18e664c67d99577ce06/.bazelrc",
    image: require("../../../static/img/oss/bazel-ios_rules_ios.png"),
  },
  {
    name: "aya-rs/bpf-linker",
    description: "A static linker for Berkeley Packet Filter programs.",
    evidenceUrl:
      "https://github.com/aya-rs/bpf-linker/blob/d694e9acfd050d843b89b237c8b223550797b74a/.github/workflows/ci.yml",
    image: require("../../../static/img/oss/aya-rs_bpf-linker.png"),
  },
  {
    name: "player-ui/player",
    description: "A cross-platform, server-driven UI framework.",
    evidenceUrl:
      "https://github.com/player-ui/player/blob/77b24b531286f72c7ab5ecebe02abfdce5223dc5/.circleci/config.yml",
    image: require("../../../static/img/oss/player-ui_player.png"),
  },
  {
    name: "sourcegraph/scip-clang",
    description: "A precise Clang-based code indexer for C, C++, and CUDA.",
    evidenceUrl:
      "https://github.com/sourcegraph/scip-clang/blob/90dbe3f59c22c89d12e78efc6afc4d0b30b93d3b/.bazelrc",
    image: require("../../../static/img/oss/sourcegraph_scip-clang.png"),
  },
  {
    name: "tweag/rules_sh",
    description: "Shell rules for Bazel.",
    evidenceUrl:
      "https://github.com/tweag/rules_sh/blob/97ec9d7de206d8ebd1bdfbbc0b98dfb3e0cb9437/.bazelrc",
    image: require("../../../static/img/oss/tweag_rules_sh.png"),
  },
  {
    name: "jvolkman/rules_pycross",
    description: "Bazel rules for cross-platform Python external dependencies.",
    evidenceUrl:
      "https://github.com/jvolkman/rules_pycross/blob/c5de55b952c0c278c91e7903069dd97d8b884260/.bazelrc",
    image: require("../../../static/img/oss/jvolkman_rules_pycross.png"),
  },
  {
    name: "keith/rules_multirun",
    description: "Bazel rules for running multiple commands in parallel in one invocation.",
    evidenceUrl:
      "https://github.com/keith/rules_multirun/blob/3969ec9c62d016e0933f6097ab1e813d927ec17d/.bazelrc",
    image: require("../../../static/img/oss/keith_rules_multirun.png"),
  },
  {
    name: "hermeticbuild/rules_rs",
    description: "Hermetic Bazel rules and toolchains for Rust projects.",
    evidenceUrl:
      "https://github.com/hermeticbuild/rules_rs/blob/c86afd0a53f91f91130146087ab78899e656a165/.github/workflows/ci.yaml",
    image: require("../../../static/img/oss/hermeticbuild_rules_rs.png"),
  },
  {
    name: "thundergolfer/example-bazel-monorepo",
    description: "An example Bazel monorepo spanning Go, Java, Python, Scala, and TypeScript.",
    evidenceUrl:
      "https://github.com/thundergolfer/example-bazel-monorepo/blob/7f3e3b4a104564f1cb20581f0c541fe64977777a/README.md#build-observability--analysis",
    image: require("../../../static/img/oss/thundergolfer_example-bazel-monorepo.png"),
  },
  {
    name: "antmicro/distant-bes",
    description: "A Python library for sending build results to services implementing Bazel's Build Event Protocol.",
    evidenceUrl:
      "https://github.com/antmicro/distant-bes/blob/fbd4f684c02c1c1e8f829ae2b4ca97111ddee1a5/README.md#compatibility",
    image: require("../../../static/img/oss/antmicro_distant-bes.png"),
  },
  {
    name: "apple-cross-toolchain/rules_applecross",
    description: "Bazel toolchains for building Apple apps and frameworks on Linux.",
    evidenceUrl:
      "https://github.com/apple-cross-toolchain/rules_applecross/blob/be1dcd7452a0924404c23603217263b4b61bc045/README.md#remote-build-execution-setup-for-buildbuddy",
    image: require("../../../static/img/oss/apple-cross-toolchain_rules_applecross.png"),
  },
  {
    name: "CaperAi/branchpoke",
    description: "A tool that reminds developers via Slack to clean up stale or merged GitLab branches.",
    evidenceUrl:
      "https://github.com/CaperAi/branchpoke/blob/cfbb203fc77b159540918a2a06091333e265e430/.bazelrc",
    image: require("../../../static/img/oss/CaperAi_branchpoke.png"),
  },
  {
    name: "curtismuntz/witness",
    description: "A webcam service controlled through an API for recording and debugging robotics tests.",
    evidenceUrl:
      "https://github.com/curtismuntz/witness/blob/474a90ebd42ffa965f4869431a675005e8f149fd/.bazelrc",
    image: require("../../../static/img/oss/curtismuntz_witness.png"),
  },
  {
    name: "dvulpe/bazel-terraform-rules",
    description: "Experimental Bazel rules for formatting, linting, testing, and publishing Terraform modules.",
    evidenceUrl:
      "https://github.com/dvulpe/bazel-terraform-rules/blob/f572a2a40d7d48b2d4a2ef04f8e6e41b2aaf3909/.bazelrc",
    image: require("../../../static/img/oss/dvulpe_bazel-terraform-rules.png"),
  },
  {
    name: "grailbio/rules_r",
    description: "Bazel rules for building and testing R packages in multi-language monorepos.",
    evidenceUrl:
      "https://github.com/grailbio/rules_r/blob/20623eca1349d8c98c2b3fa73bba8c5eb70aa778/tests/buildbuddy.bazelrc",
    image: require("../../../static/img/oss/grailbio_rules_r.png"),
  },
  {
    name: "hdl/bazel_rules_hdl",
    description: "Bazel rules for hardware design using open tools such as Yosys, Verilator, and OpenROAD.",
    evidenceUrl:
      "https://github.com/hdl/bazel_rules_hdl/blob/ece083ef1385ff9ab067711fa766d77c3b54d961/.bazelrc",
    image: require("../../../static/img/oss/hdl_bazel_rules_hdl.png"),
  },
  {
    name: "nitnelave/lru_cache",
    description: "A C++17 LRU cache library with configurable storage backends.",
    image: require("../../../static/img/oss/nitnelave_lru_cache.png"),
  },
  {
    name: "samhowes/rules_msbuild",
    description: "Bazel integration for .NET projects using MSBuild.",
    evidenceUrl:
      "https://github.com/samhowes/rules_msbuild/blob/972c6497c219828c4cf89366af3dd541256fbeb0/.buildbuddy/make_rc.sh",
    image: require("../../../static/img/oss/samhowes_rules_msbuild.png"),
  },
  {
    name: "samhowes/rules_tsql",
    description: "Bazel rules for compiling and deploying T-SQL database packages across platforms.",
    evidenceUrl:
      "https://github.com/samhowes/rules_tsql/blob/55ca4001b6436b9b7d34abbee9e796d4236bf725/.ci/init.sh",
    image: require("../../../static/img/oss/samhowes_rules_tsql.png"),
  },
];

type Props = {
  length?: number;
};

function Component({ length }: Props) {
  const displayedRepos = typeof length === "number" ? repos.slice(0, length) : repos;

  return (
    <div className={styles.repos}>
      {displayedRepos.map((repo) => {
        const repositoryUrl = `https://github.com/${repo.name}`;

        return (
          <article className={styles.repo} key={repo.name}>
            <a
              aria-label={`View ${repo.name} on GitHub`}
              className={styles.repoImageLink}
              href={repositoryUrl}
              rel="noopener noreferrer"
              target="_blank">
              <Image
                alt={`${repo.name} repository preview`}
                className={styles.repoImage}
                img={repo.image}
                shouldAutoDownload={() => true}
                threshold={10000}
              />
            </a>
            <h3 className={styles.repoTitle}>{repo.name}</h3>
            <p className={styles.repoDescription}>{repo.description}</p>
          </article>
        );
      })}
    </div>
  );
}

export default Component;
