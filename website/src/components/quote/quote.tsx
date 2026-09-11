import React from "react";
import common from "../../css/common.module.css";
import styles from "./quote.module.css";

const tweets = [
  {
    name: "Narendra Patwardhan",
    handle: "@overlordayn",
    avatar: "/img/tweets/overlordayn.jpg",
    url: "https://x.com/overlordayn/status/2077101433942343888",
    text: `BuildBuddy is literally magic. If I ran the stuff it gets done in minutes on my laptop, it would explode.`,
  },
  {
    name: "Glenn Sonna",
    handle: "@GlennSonna",
    avatar: "/img/tweets/GlennSonna.jpg",
    url: "https://x.com/GlennSonna/status/2083278509892391220",
    text: `We migrated our native CI to @bazelbuild + remote execution on @buildbuddy.

Median Android native build: 31.5m → 1.8m (-94%)
Full end-to-end gate: 43.8m → 12.1m (-72%)`,
  },
  {
    name: "Corentin Kérisit",
    handle: "@corentinanjuna",
    avatar: "/img/tweets/corentinanjuna.jpg",
    url: "https://x.com/corentinanjuna/status/2092456143935463457",
    text: `This process is so heavy that it would be simply impracticable to do in normal circumstances.

This is made possible thanks to Remote Builds distributed on thousands of cores using @buildbuddy.

It's done as part of the same graph as the final target. This is madness...`,
  },
  {
    name: "Bazel",
    handle: "@bazelbuild",
    avatar: "/img/tweets/bazelbuild.jpg",
    url: "https://x.com/bazelbuild/status/1987943981812965522",
    text: `Thank you again to @buildbuddy, the Platinum sponsor of #BazelCon 2025! The expertise you share with the Bazel community is invaluable.

Listen to Fabian's talk, Rootcausing Rebuilds with “bb explain,” on Tuesday, Nov 11th!`,
  },
  {
    name: "Steeve Morin",
    handle: "@steeve",
    avatar: "/img/tweets/steeve.jpg",
    url: "https://x.com/steeve/status/2022971347757142032",
    text: `My friends who are actually working *on* LLVM are the ones complaining.

Btw, thanks to @corentinanjuna and @buildbuddy a *cold* LLVM build is now 20s, fully cross compiled from *anything* to *anything* (yes even macOS).`,
  },
  {
    name: "AJ Taylor",
    handle: "@0xAnthon",
    avatar: "/img/tweets/0xAnthon.jpg",
    url: "https://x.com/0xAnthon/status/2086912764132610086",
    text: `It's worth the investment to dial in your remote build execution setup

We use buildbuddy, bazel, and nix @etherfuse and the devex difference is night and day`,
  },
  {
    name: "0xפנתן‎",
    handle: "@p_nathan",
    avatar: "/img/tweets/p_nathan.jpg",
    url: "https://x.com/p_nathan/status/1952776520717734397",
    text: `If it was me, I'd use Bazel for that with @buildbuddy - which integrates well with GitHub. Otherwise the Gitlab runners burn minutes waiting for the buildbuddy run to complete.`,
  },
  {
    name: "Spotify Engineering",
    handle: "@SpotifyEng",
    avatar: "/img/tweets/SpotifyEng.jpg",
    url: "https://x.com/SpotifyEng/status/1714358648736977064",
    text: `See how @BalestraPatrick and his team migrated the @Spotify iOS app with @Bazelbuild & @buildbuddy, cutting our build times by up to 4X. 🤯`,
  },
  {
    name: "mfreeman451",
    handle: "@mfreeman451",
    avatar: "/img/tweets/mfreeman451.jpg",
    url: "https://x.com/mfreeman451/status/1981045685324828867",
    text: `We just moved from GitHub actions to bazel and buildbuddy, our build times went from 2.5hrs down to 6 minutes.`,
  },
  {
    name: "本味纳凉地（original-cooling-space）",
    handle: "@originalcspace",
    avatar: "/img/tweets/originalcspace.jpg",
    url: "https://x.com/originalcspace/status/2092166704411414822",
    text: `I followed the official guide to set up BuildBuddy—which you recommended yesterday—this afternoon. The build speed is dozens of times faster, and I haven't encountered any issues like processes abruptly terminating in my wsl2 environment. Thanks for your help!`,
  },
  {
    name: "iqbal syamil",
    handle: "@iqbalsyaa",
    avatar: "/img/tweets/iqbalsyaa.jpg",
    url: "https://x.com/iqbalsyaa/status/1792888831450193968",
    text: `So I'm trying remote cache for Bazel using buildbuddy & integrating it with github ci

Not so optimized (only for POC purposes) but
13m -> 9s

this thing was blazingly smart & fast wtf`,
  },
  {
    name: "Farid Zakaria",
    handle: "@fmzakari",
    avatar: "/img/tweets/fmzakari.jpg",
    url: "https://x.com/fmzakari/status/1671200112423600129",
    text: `@buildbuddy sometimes I am frustrated with Bazel but what's keeping me sane and HAPPY is working with BuildBuddy.

Just turned on workflows and it was painless -- turned off my GitHub action in favor of it.`,
  },
];

function renderText(text: string) {
  return text.split(/(@?buildbuddy)/gi).map((part, index) =>
    /^@?buildbuddy$/i.test(part) ? (
      <strong className={styles.buildBuddy} key={index}>
        {part}
      </strong>
    ) : (
      part
    )
  );
}

function Component() {
  return (
    <section className={`${common.section} ${common.sectionDark} ${common.sectionRounded}`}>
      <div className={`${common.container} ${styles.container}`}>
        <div className={styles.heading}>
          <div className={common.pillTitle}>From the community</div>
          <h2 className={styles.title}>What developers are saying</h2>
        </div>
        <div className={styles.wall}>
          {tweets.map((tweet) => (
            <a
              className={`${styles.card} ${
                tweet.handle === "@steeve" || tweet.handle === "@mfreeman451" ? styles.columnStart : ""
              }`}
              href={tweet.url}
              key={tweet.url}
              rel="noopener noreferrer"
              target="_blank">
              <article>
                <header className={styles.author}>
                  <img alt="" className={styles.avatar} height="44" loading="lazy" src={tweet.avatar} width="44" />
                  <div className={styles.authorText}>
                    <div className={styles.name}>{tweet.name}</div>
                    <div className={styles.handle}>{tweet.handle}</div>
                  </div>
                </header>
                <p className={styles.text}>{renderText(tweet.text)}</p>
              </article>
            </a>
          ))}
        </div>
      </div>
    </section>
  );
}

export default Component;
