import React, { useState } from "react";
import Layout from "@theme/Layout";
import Hero from "../components/hero/hero";
import CTA from "../components/cta/cta";
import contact from "./contact.module.css";
import common from "../css/common.module.css";
import styles from "./plugins.module.css";
import message from "../util/message";
import { Package, ArrowUpCircle, Palette, Highlighter, Globe, Bell, Network, Brush } from "lucide-react";
import { copyToClipboard } from "../util/clipboard";

let plugins = [
  {
    title: "open-invocation",
    description: (
      <>
        Adds support for an `--open` argument which automatically opens your BuildBuddy invocation page once your build
        completes.
      </>
    ),
    icon: <ArrowUpCircle />,
    snippet: "bb install buildbuddy-io/plugins:open-invocation",
    docs: "https://github.com/buildbuddy-io/buildbuddy/tree/master/cli/plugins/open-invocation#readme",
    copied: false,
  },
  {
    title: "go-deps",
    description: "Prompts you to automatically run gazelle when you forget to update your BUILD files.",
    icon: <Network />,
    snippet: "bb install buildbuddy-io/plugins:go-deps",
    docs: "https://github.com/buildbuddy-io/buildbuddy/tree/master/cli/plugins/go-deps#readme",
    copied: false,
  },
  {
    title: "theme-modern",
    description: "Styles your Bazel console output with a more modern theme.",
    icon: <Palette />,
    snippet: "bb install siggisim/theme-modern",
    docs: "https://github.com/siggisim/theme-modern#readme",
    copied: false,
  },
  // {
  //   title: "sane-defaults",
  //   description: "Sets a set of default Bazel flags that should probably be set by default.",
  //   icon: <Brush />,
  //   snippet: "bb install buildbuddy-io/plugins:sane-defaults",
  //   docs: "https://github.com/buildbuddy-io/plugins/tree/main/plugins/open-invocation",
  //   copied: false,
  // },
  {
    title: "notify",
    description:
      "Adds support for a `--notify` argument that sends you a desktop notification when your build completes.",
    icon: <Bell />,
    snippet: "bb install buildbuddy-io/plugins:notify",
    docs: "https://github.com/buildbuddy-io/buildbuddy/tree/master/cli/plugins/notify#readme",
    copied: false,
  },
  {
    title: "theme-mono",
    description: "Styles your Bazel console output with a minimalist, monochrome theme.",
    icon: <Brush />,
    snippet: "bb install siggisim/theme-mono",
    docs: "https://github.com/siggisim/theme-mono#readme",
    copied: false,
  },
  {
    title: "go-highlight",
    description:
      "Highlights any go errors to make them easier to visually distinguish from the rest of your console output.",
    icon: <Highlighter />,
    snippet: "bb install bduffany/go-highlight",
    docs: "https://github.com/bduffany/go-highlight#readme",
    copied: false,
  },
  {
    title: "ping-remote",
    description: "Disables remote execution if not connected to the internet.",
    icon: <Globe />,
    snippet: "bb install siggisim/ping-remote",
    docs: "https://github.com/siggisim/ping-remote#readme",
    copied: false,
  },
];

let form = {
  name: React.createRef<HTMLInputElement>(),
  email: React.createRef<HTMLInputElement>(),
  pluginName: React.createRef<HTMLInputElement>(),
  pluginDesc: React.createRef<HTMLInputElement>(),
  pluginLink: React.createRef<HTMLInputElement>(),
  button: React.createRef<HTMLButtonElement>(),
};

function Component() {
  let [count, setCount] = useState(0);
  return (
    <Layout title="BuildBuddy Plugin Library">
      <div className={common.page}>
        <Hero
          title="Plugin Library"
          subtitle="Add functionality and customize your BuildBuddy CLI experience with plugins built by our incredible developer community."
          primaryButtonText="Create your own plugin"
          primaryButtonHref="/docs/cli-plugins#creating-a-plugin"
          secondaryButtonText="Plugin docs"
          secondaryButtonHref="/docs/cli-plugins"
          style={{ padding: "64px 0", minHeight: "0" }}
          component={
            <div className={styles.featuredPlugin}>
              <div className={styles.sectionTitle}>Featured plugin</div>
              <div
                onClick={() => handlePluginClicked(plugins[0], count, setCount)}
                className={`${styles.plugin} ${(plugins[0].copied && styles.copied) || ""}`}>
                <div className={styles.pluginIcon}>{plugins[0].icon || <Package />}</div>
                <div className={styles.pluginTitle}>{plugins[0].title}</div>
                <div className={styles.pluginDescription}>{plugins[0].description}</div>
                <div className={styles.pluginSnippet}>{plugins[0].snippet}</div>
                <div className={styles.pluginButtons}>
                  <a target="_blank" href={plugins[0].docs} onClick={(e) => e.stopPropagation()}>
                    View docs
                  </a>
                </div>
              </div>
            </div>
          }
        />

        <div className={styles.plugins}>
          <div className={styles.sectionTitle}>Top plugins</div>
          {plugins.slice(1).map((plugin) => (
            <div
              className={`${styles.plugin} ${(plugin.copied && styles.copied) || ""}`}
              onClick={() => handlePluginClicked(plugin, count, setCount)}>
              <div className={styles.pluginIcon}>{plugin.icon || <Package />}</div>
              <div className={styles.pluginTitle}>{plugin.title}</div>
              <div className={styles.pluginDescription}>{plugin.description}</div>
              <div className={styles.pluginSnippet}>{plugin.snippet}</div>
              <div className={styles.pluginButtons}>
                <a target="_blank" href={plugin.docs} onClick={(e) => e.stopPropagation()}>
                  View docs
                </a>
              </div>
            </div>
          ))}
        </div>

        <div className={`${common.section} ${common.sectionDark} ${styles.ossSection}`}>
          <div className={`${common.container} ${common.splitContainer}`}>
            <div className={common.text}>
              <h1 id="share" className={common.title}>
                Got a plugin you want to share?
              </h1>
              <div className={common.subtitle}>
                Help others improve their build experience by sharing your plugin in the plugin library!{" "}
              </div>
            </div>
            <div className={contact.form}>
              <input ref={form.name} placeholder="Your name" />
              <input ref={form.email} placeholder="Email address" />
              <input ref={form.pluginName} placeholder="Plugin name" className={contact.span2} />
              <input ref={form.pluginDesc} placeholder="Plugin description" className={contact.span2} />
              <input ref={form.pluginLink} placeholder="Repo link" className={contact.span2} />
              <button
                ref={form.button}
                onClick={() => sendMessage()}
                className={`${common.button} ${common.buttonPrimary} ${contact.span2}`}>
                Submit your plugin
              </button>
            </div>
          </div>
        </div>

        <CTA href="/docs/cli" text="Get the BuildBuddy CLI" />
      </div>
    </Layout>
  );
}

function handlePluginClicked(plugin, count, setCount) {
  plugin.copied = true;
  setCount(count + 1);

  copyToClipboard(plugin.snippet);

  setTimeout(() => {
    plugin.copied = false;
    setCount(count - 1);
  }, 2000);
}

function sendMessage() {
  message(
    `New Plugin Submitted!\nName: ${form.name.current.value}\nEmail: ${form.email.current.value}\nPlugin name: ${form.pluginName.current.value}\nPlugin des: ${form.pluginDesc.current.value}\nPlugin link: ${form.pluginLink.current.value}`
  );

  form.name.current.disabled = true;
  form.email.current.disabled = true;
  form.pluginName.current.disabled = true;
  form.pluginDesc.current.disabled = true;
  form.pluginLink.current.disabled = true;

  form.button.current.innerText = "Plugin Submitted!";
  form.button.current.disabled = true;
}

export default Component;
