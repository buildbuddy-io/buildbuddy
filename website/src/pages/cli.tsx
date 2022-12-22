import React from "react";
import Layout from "@theme/Layout";
import common from "../css/common.module.css";

import Hero from "../components/hero/hero";
import CTA from "../components/cta/cta";
import Terminal, { Prompt, Info, Error, Question, Detail } from "../components/terminal/terminal";

function Component() {
  return (
    <Layout title="BuildBuddy CLI">
      <div className={common.page}>
        <Hero
          title="BuildBuddy CLI"
          subtitle="Take BuildBuddy to the command line &mdash; built on top of Bazelisk with support for plugins, authentication, flaky network conditions, and so much more."
          component={
            <Terminal
              duration={1200}
              states={[
                <Prompt />,
                <>{command}</>,
                <>
                  {command}
                  {start}
                </>,
                <>
                  {command}
                  {start}
                  {error}
                </>,
                <>
                  {command}
                  {start}
                  {error}
                  {completed}
                </>,

                <>
                  {command}
                  {start}
                  {error}
                  {completed}
                  <div>
                    <Question text={"> Run gazelle to fix these packages?"} /> <Detail text="(yes)/always/no/never:" />
                  </div>
                </>,
                <>
                  {command}
                  {start}
                  {error}
                  {completed}
                  <div>
                    <Question text={"> Run gazelle to fix these packages?"} /> <Detail text="(yes)/always/no/never:" />{" "}
                    yes
                  </div>
                </>,
                <>
                  {command}
                  {start}
                  {error}
                  {completed}
                  <div>
                    <Question text={"> Run gazelle to fix these packages?"} /> <Detail text="(yes)/always/no/never:" />{" "}
                    yes
                  </div>
                  <div>
                    <Info text={"> bazel run //:gazelle -- server/util/authutil"} /> 🛠️ fixing...
                  </div>
                </>,
                <>
                  {command}
                  {start}
                  {error}
                  {completed}
                  <div>
                    <Question text={"> Run gazelle to fix these packages?"} /> <Detail text="(yes)/always/no/never:" />{" "}
                    yes
                  </div>
                  <div>
                    <Info text={"> bazel run //:gazelle -- server/util/authutil"} /> ✅ fix applied
                  </div>
                </>,
              ]}
            />
          }
          bigImage={true}
          lessPadding={true}
          snippet={"curl -fsSL install.buildbuddy.io | bash"}
          primaryButtonText=""
          secondaryButtonText="View docs"
          secondaryButtonHref="/docs/cli"
        />

        <Hero
          title="Command line compatible with Bazel"
          subtitle={
            <>
              Just like with Bazelisk, you can simply <code>alias bazel=bb</code> and keep using bazel the way you
              normally would. It's also written in go, fully open source, and MIT licensed.
            </>
          }
          component={
            <Terminal
              duration={1200}
              states={[
                <Prompt />,
                <>
                  <div>
                    <Prompt />
                    <span>alias bazel=bb</span>
                  </div>
                </>,
                <>
                  <div>
                    <Prompt />
                    <span>alias bazel=bb</span>
                  </div>
                  <div>
                    <Prompt />
                  </div>
                </>,
                <>
                  <div>
                    <Prompt />
                    <span>alias bazel=bb</span>
                  </div>
                  <div>
                    <Prompt />
                    <span>bazel build //...</span>
                  </div>
                </>,
                <>
                  <div>
                    <Prompt />
                    <span>alias bazel=bb</span>
                  </div>
                  <div>
                    <Prompt />
                    <span>bazel build //...</span>
                  </div>
                  {success}
                </>,
              ]}
            />
          }
          bigImage={true}
          lessPadding={true}
          primaryButtonText="See the code"
          primaryButtonHref="https://github.com/buildbuddy-io/buildbuddy/tree/master/cli"
          secondaryButtonText="View the docs"
          secondaryButtonHref="/docs/cli"
        />

        <Hero
          title={
            <>
              Slow network?
              <br />
              No network?
              <br />
              No problem!
            </>
          }
          subtitle="BuildBuddy CLI routes your BuildBuddy requests through a local proxy, so you never have to wait for slow uploads and flaky network conditions will never fail or slow down your builds."
          component={
            <Terminal
              duration={1200}
              states={[
                <Prompt />,
                <>
                  <Prompt />
                  <span>sudo ifconfig en1 down</span>
                </>,
                <>
                  <Prompt />
                  <span>sudo ifconfig en1 down</span>
                  <div>
                    <Prompt />
                  </div>
                </>,
                <>
                  <Prompt />
                  <span>sudo ifconfig en1 down</span>
                  <div>
                    <Prompt /> bb build server --config=cache
                  </div>
                </>,
                <>
                  <Prompt />
                  <span>sudo ifconfig en1 down</span>
                  <div>
                    <Prompt /> bb build server --config=cache
                  </div>
                  {success}
                </>,
              ]}
            />
          }
          bigImage={true}
          lessPadding={true}
          flipped={false}
          primaryButtonText=""
          secondaryButtonText=""
        />

        <Hero
          title="Powerful Plugins"
          subtitle="Dead simple to write, even easier to install, use and share."
          component={
            <Terminal
              duration={1200}
              states={[
                <Prompt />,
                <>
                  <div>
                    <Prompt />
                    <span>{`bb install buildbuddy-io/plugins@v0.0.14:open-invocation`}</span>
                  </div>
                </>,
                <>
                  <div>
                    <Prompt />
                    <span>{`bb install buildbuddy-io/plugins@v0.0.14:open-invocation`}</span>
                  </div>
                  <div>
                    <Prompt />
                  </div>
                </>,
                <>
                  <div>
                    <Prompt />
                    <span>{`bb install buildbuddy-io/plugins@v0.0.14:open-invocation`}</span>
                  </div>
                  <div>
                    <Prompt />
                    <span>bazel build //... --open</span>
                  </div>
                </>,
                <>
                  <div>
                    <Prompt />
                    <span>{`bb install buildbuddy-io/plugins@v0.0.14:open-invocation`}</span>
                  </div>
                  <div>
                    <Prompt />
                    <span>bazel build //... --open</span>
                  </div>
                  {success}
                </>,
                <>
                  <div>
                    <Prompt />
                    <span>{`bb install buildbuddy-io/plugins@v0.0.14:open-invocation`}</span>
                  </div>
                  <div>
                    <Prompt />
                    <span>bazel build //... --open</span>
                  </div>
                  {success}
                  <Detail
                    text={"> Opening https://app.buildbuddy.io/invocation/b9b09691-148c-4cf5-9d31-aa428023e9c4"}
                  />
                </>,
              ]}
            />
          }
          bigImage={true}
          lessPadding={true}
          flipped={false}
          snippet={`bb install buildbuddy-io/plugins@v0.0.14:open-invocation`}
          primaryButtonText="See plugin library"
          primaryButtonHref="/plugins"
          secondaryButtonText="View docs"
          secondaryButtonHref="/docs/cli-plugins"
        />

        <Hero
          title="Super Simple Auth"
          subtitle="No more fiddling with certs or headers. Login to BuildBuddy with a single command that authenticates all of your remote requests."
          component={
            <Terminal
              duration={1200}
              states={[
                <Prompt />,
                <>
                  <Prompt />
                  <span>bb login</span>
                </>,
                <>
                  <Prompt />
                  <span>bb login</span>
                  <div>Enter your api key from https://app.buildbuddy.io/settings/org/api-keys</div>
                </>,
                <>
                  <Prompt />
                  <span>bb login</span>
                  <div>Enter your api key from https://app.buildbuddy.io/settings/org/api-keys</div>
                  <div>MYHUNTER2INGAPIKEY</div>
                </>,
                <>
                  <Prompt />
                  <span>bb login</span>
                  <div>Enter your api key from https://app.buildbuddy.io/settings/org/api-keys</div>
                  <div>MYHUNTER2INGAPIKEY</div>
                  <div>You are now logged in!</div>
                  <Prompt />
                </>,
              ]}
            />
          }
          bigImage={true}
          lessPadding={true}
          flipped={false}
          primaryButtonText=""
          secondaryButtonText=""
        />

        <Hero
          title="Bring your whole project onboard"
          subtitle={
            <>
              If you're already using Bazelisk, just add one line to your <code>.bazelversion</code> file and you're off
              to the races!
            </>
          }
          component={
            <Terminal
              duration={1200}
              states={[
                <Prompt />,
                <>
                  <div>
                    <Prompt />
                    <span>{`echo "$(echo "buildbuddy-io/0.1.4"; cat .bazelversion)" > .bazelversion`}</span>
                  </div>
                </>,
                <>
                  <div>
                    <Prompt />
                    <span>{`echo "$(echo "buildbuddy-io/0.1.4"; cat .bazelversion)" > .bazelversion`}</span>
                  </div>
                  <div>
                    <Prompt />
                  </div>
                </>,
                <>
                  <div>
                    <Prompt />
                    <span>{`echo "$(echo "buildbuddy-io/0.1.4"; cat .bazelversion)" > .bazelversion`}</span>
                  </div>
                  <div>
                    <Prompt />
                    <span>bazel build //...</span>
                  </div>
                </>,
                <>
                  <div>
                    <Prompt />
                    <span>{`echo "$(echo "buildbuddy-io/0.1.4"; cat .bazelversion)" > .bazelversion`}</span>
                  </div>
                  <div>
                    <Prompt />
                    <span>bazel build //...</span>
                  </div>
                  {success}
                </>,
              ]}
            />
          }
          bigImage={true}
          lessPadding={true}
          flipped={false}
          snippet={`echo "$(echo "buildbuddy-io/0.1.4"; cat .bazelversion)" > .bazelversion`}
          primaryButtonText=""
          secondaryButtonText=""
        />

        <CTA href="/docs/cli" text="View the installation docs" />
      </div>
    </Layout>
  );
}

let command = (
  <>
    <Prompt />
    <span>bb build server</span>
  </>
);

let start = (
  <>
    <div>
      <Info /> Streaming build results to: https://app.buildbuddy.io/invocation/2124a2c3-5240-4bdd-8aa1-5303b86d2b11
    </div>
    <div>
      <Info /> Analyzed target //server:server (1 packages loaded, 20 targets configured).
    </div>
    <div>
      <Info /> Found 1 target...
    </div>
  </>
);

let error = (
  <>
    {" "}
    <div>
      <Error /> /Users/siggi/Code/buildbuddy/server/util/authutil/BUILD:3:11: GoCompilePkg
      server/util/authutil/authutil.a failed: (Exit 1): builder failed: error executing command
      bazel-out/darwin_arm64-opt-exec-2B5CBBC6/bin/external/go_sdk_darwin_arm64/builder compilepkg -sdk
      external/go_sdk_darwin_arm64 -installsuffix darwin_arm64 -src server/util/authutil/authutil.go -arc ... (remaining
      19 arguments skipped)
    </div>
    <div>
      Use --sandbox_debug to see verbose messages from the sandbox and retain the sandbox build root for debugging
    </div>
    <div>compilepkg: missing strict dependencies:</div>
    <div>
      {" "}
      /private/var/tmp/_bazel_siggi/e1a494dcbcb7b7235bc655bacb26413a/sandbox/darwin-sandbox/1739/execroot/buildbuddy/server/util/authutil/authutil.go:
      import of "github.com/buildbuddy-io/buildbuddy/server/interfaces"
    </div>
    <div>No dependencies were provided.</div>
    <div>Check that imports in Go sources match importpath attributes in deps.</div>
    <div>Target //server/cmd/buildbuddy:buildbuddy failed to build</div>
    <div>Use --verbose_failures to see the command lines of failed build steps.</div>
  </>
);

let completed = (
  <>
    {" "}
    <div>
      <Info /> Elapsed time: 0.424s, Critical Path: 0.07s
    </div>
    <div>
      <Info /> 2 processes: 2 internal.
    </div>
    <div>
      <Info /> Streaming build results to: https://app.buildbuddy.io/invocation/2124a2c3-5240-4bdd-8aa1-5303b86d2b11
    </div>
    <div>
      <Error text="FAILED:" /> Build did NOT complete successfully
    </div>
  </>
);

let success = (
  <>
    <div>
      <Info /> Streaming build results to: https://app.buildbuddy.io/invocation/b9b09691-148c-4cf5-9d31-aa428023e9c4
    </div>
    <div>
      <Info /> Analyzed target //server:server (0 packages loaded, 0 targets configured).
    </div>
    <div>
      <Info /> INFO: Found 1 target...{" "}
    </div>
    <div>
      Target //server/cmd/buildbuddy:buildbuddy up-to-date: <br />
      bazel-bin/server/cmd/buildbuddy/buildbuddy_/buildbuddy
    </div>
    <div>
      <Info /> Elapsed time: 0.640s, Critical Path: 0.05s INFO: 1 process: 1 internal.
    </div>
    <div>
      <Info /> Streaming build results to: https://app.buildbuddy.io/invocation/b9b09691-148c-4cf5-9d31-aa428023e9c4
    </div>
    <div>
      <Info /> Build completed successfully, 1 total action
    </div>
  </>
);

export default Component;
