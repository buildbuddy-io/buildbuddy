import React from "react";
import Layout from "@theme/Layout";
import styles from "./pricing.module.css";
import common from "../css/common.module.css";
import { Check, CheckCircle2 } from "lucide-react";
import CTA from "../components/cta/cta";

function Index() {
  return (
    <Layout title="Pricing">
      <div className={common.page}>
        <div className={common.section}>
          <div className={common.container}>
            <div className={common.centeredText}>
              <div className={common.title}>Flexible pricing that is ready to scale with your business.</div>
            </div>
          </div>
        </div>
        <div className={common.container}>
          <div className={styles.priceTiers}>
            <div className={styles.priceTier}>
              <div className={styles.priceTierTitle}>Personal</div>
              <div className={styles.priceTierDescription}>
                Helping individual developers, small teams, and open source projects view, debug, analyze, and speed up
                their builds.
              </div>
              <div className={styles.price}>Free</div>
              <a
                style={{ backgroundColor: "#607D8B", border: "0", color: "#fff" }}
                className={common.button}
                href="https://app.buildbuddy.io">
                Get Started for Free
              </a>
              <div className={styles.priceTierFeatures}>
                <li>
                  <CheckCircle2 /> For small teams and open source projects
                </li>
                <li>
                  <CheckCircle2 /> 100 GB of cache transfer
                </li>
                <li>
                  <CheckCircle2 /> Up to 80 cores for remote builds
                </li>
                <li>
                  <CheckCircle2 /> Community support
                </li>
              </div>
            </div>

            <div className={styles.priceTier}>
              <div className={styles.priceTierTitle}>Team</div>
              <div className={styles.priceTierDescription}>Team Description</div>
              <div className={styles.price}>Pay-as-you-go</div>
              <a
                className={common.button}
                style={{ backgroundColor: "#2196F3", border: "0", color: "#fff" }}
                href="https://app.buildbuddy.io">
                Get Started for Free
              </a>
              <div className={styles.priceTierFeatures}>
                <li>
                  <CheckCircle2 /> For small teams and startups
                </li>
                <li>
                  <CheckCircle2 /> $X / GB of cache transfer over 100 GB
                </li>
                <li>
                  <CheckCircle2 /> Up to 800 cores for remote builds
                </li>
                <li>
                  <CheckCircle2 /> Email support
                </li>
              </div>
            </div>

            <div className={styles.priceTier}>
              <div className={styles.priceTierTitle}>Enterprise</div>
              <div className={styles.priceTierDescription}>
                Empowering companies with custom Bazel solutions tailored to the unique requirements of their business.
              </div>
              <div className={styles.price}>Suited for your business</div>
              <a className={`${common.button} ${common.buttonPrimary}`} href="/request-quote">
                Request a Quote
              </a>
              <div className={styles.priceTierFeatures}>
                <li>
                  <CheckCircle2 /> Maximize developer productivity
                </li>
                <li>
                  <CheckCircle2 /> SSO/SAML
                </li>
                <li>
                  <CheckCircle2 /> Unlimited Cores of RBE
                </li>
                <li>
                  <CheckCircle2 /> Isolated infrastructure
                </li>
                <li>
                  <CheckCircle2 /> Dedicated support engineer & SLAs
                </li>
              </div>
            </div>
          </div>
        </div>
        <div className={common.container}>
          <div className={common.centeredText}>
            <div className={common.title}>Compare plans & features</div>
          </div>
        </div>
        <div className={common.container}>
          <div className={styles.featureGrid}>
            <div className={styles.featureGridHeader}>Basics</div>
            <div className={styles.featureGridHeader}>Personal</div>
            <div className={styles.featureGridHeader}>Team</div>
            <div className={styles.featureGridHeader}>Enterprise</div>

            <div>Users</div>
            <div>10</div>
            <div>Unlimited</div>
            <div>Unlimited</div>

            <div>Builds</div>
            <div>Unlimited</div>
            <div>Unlimited</div>
            <div>Unlimited</div>

            <div>SSO / SAML</div>
            <No />
            <No />
            <Yes />

            <div>Invoice billing</div>
            <No />
            <No />
            <Yes />

            <div className={styles.featureGridHeader}>Features</div>
            <div className={styles.featureGridHeader}>Personal</div>
            <div className={styles.featureGridHeader}>Team</div>
            <div className={styles.featureGridHeader}>Enterprise</div>

            <div>Remote execution</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Remote caching</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Build & test results UI</div>
            <Yes />
            <Yes />
            <Yes />

            <div>API Access</div>
            <Yes />
            <Yes />
            <Yes />

            <div className={styles.featureGridHeader}>Remote Execution</div>
            <div className={styles.featureGridHeader}>Personal</div>
            <div className={styles.featureGridHeader}>Team</div>
            <div className={styles.featureGridHeader}>Enterprise</div>

            <div>Linux cores</div>
            <div>Up to 80</div>
            <div>Up to 800</div>
            <div>Unlimited</div>

            <div>Mac cores</div>
            <div></div>
            <div>$45 / core</div>
            <div>Unlimited</div>

            <div>Autoscaling</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Custom docker images</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Docker image caching</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Build without the bytes</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Cross-platform builds</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Bring your own runners</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Workflows</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Remote persistent workers</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Dynamic scheduling</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Sibling docker containers</div>
            <No />
            <No />
            <Yes />

            <div>Custom machines per-action</div>
            <No />
            <No />
            <Yes />

            <div className={styles.featureGridHeader}>Remote Caching</div>
            <div className={styles.featureGridHeader}>Personal</div>
            <div className={styles.featureGridHeader}>Team</div>
            <div className={styles.featureGridHeader}>Enterprise</div>

            <div>Cache transfer</div>
            <div>100 GB</div>
            <div>$X / GB</div>
            <div>Unlimited</div>

            <div>Globally distribued edge-cache</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Build without the bytes</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Custom retention period</div>
            <No />
            <No />
            <Yes />

            <div className={styles.featureGridHeader}>Results UI</div>
            <div className={styles.featureGridHeader}>Personal</div>
            <div className={styles.featureGridHeader}>Team</div>
            <div className={styles.featureGridHeader}>Enterprise</div>

            <div>Shareable build & test links</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Real-time updating</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Team build history</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Filtering & date selection</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Built-in timing profile</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Test log parsing</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Cache stats</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Downloadable artifacts</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Invocation diffing</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Trends</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Test Grid</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Action explorer</div>
            <Yes />
            <Yes />
            <Yes />

            <div className={styles.featureGridHeader}>Support</div>
            <div className={styles.featureGridHeader}>Personal</div>
            <div className={styles.featureGridHeader}>Team</div>
            <div className={styles.featureGridHeader}>Enterprise</div>

            <div>Community support</div>
            <Yes />
            <Yes />
            <Yes />

            <div>Email support</div>
            <No />
            <Yes />
            <Yes />

            <div>Dedicated support engineer</div>
            <No />
            <No />
            <Yes />

            <div>Shared Slack channel</div>
            <No />
            <No />
            <Yes />

            <div>99.9% uptime SLA</div>
            <No />
            <No />
            <Yes />
          </div>
        </div>

        <div className={common.container}>
          <div className={common.centeredText}>
            <div className={common.title}>Questions & Answers</div>
          </div>
        </div>
        <div className={common.container}>
          <div className={styles.questions}>
            <div className={styles.question}>
              <div className={styles.questionQuestion}>What happens if I go over my limit?</div>
              <div className={styles.answerAnswer}>
                We don’t apply hard limits that prevent you from using more than your plan allows. If you have a big
                temporary burst of usage, feel free. If you exceed the limits of your plan for a short while, we’ll get
                in touch about upgrading to something more suited to your needs. Feel free to{" "}
                <a href="/contact">contact sales</a> if you have questions about your use-case.
              </div>
            </div>
            <div className={styles.question}>
              <div className={styles.questionQuestion}>Which plan is right for me?</div>
              <div className={styles.answerAnswer}>
                Our Personal plan is suited for individuals small teams, and open source projects. Enterprise is for
                teams seeking greater performance, flexibility, and security.{" "}
                <a href="/request-quote">Contact our sales team</a> to learn more.
              </div>
            </div>
            <div className={styles.question}>
              <div className={styles.questionQuestion}>What are my deployment options?</div>
              <div className={styles.answerAnswer}>
                The features chart above compares BuildBuddy Cloud-hosted solutions. BuildBuddy can also be{" "}
                <a href="https://docs.buildbuddy.io/docs/on-prem">deployed on-prem</a>. We offer an MIT licensed
                open-source version of BuildBuddy targeted at individual developers. We also offer an{" "}
                <a href="https://docs.buildbuddy.io/docs/enterprise-setup">enterpise on-prem solution</a> that we offer
                either managed or self-managed. <a href="/request-quote">Contact sales</a> for more information on our
                managed on-prem offering.
              </div>
            </div>
            <div className={styles.question}>
              <div className={styles.questionQuestion}>What if I have more questions?</div>
              <div className={styles.answerAnswer}>
                We're happy to answer any questions you have. If you'd like to learn more about BuildBuddy's features,
                you can <a href="/request-demo">schedule a demo</a>. If you're curious abour pricing, you can{" "}
                <a href="/request-quote">request a quote</a>. For all other questions, you can either{" "}
                <a href="mailto:hello@buildbuddy.io">email us</a>, or{" "}
                <a href="https://slack.buildbuddy.io">chat with us on Slack</a>.
              </div>
            </div>
          </div>
        </div>

        <CTA />
      </div>
    </Layout>
  );
}

function No() {
  return <div></div>;
}

function Yes() {
  return (
    <div className={styles.check}>
      <Check color="#4CAF50" />
    </div>
  );
}

export default Index;
