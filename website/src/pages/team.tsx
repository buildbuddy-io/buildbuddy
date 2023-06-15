import React from "react";
import Layout from "@theme/Layout";
import common from "../css/common.module.css";
import styles from "./team.module.css";
import Image from "@theme/IdealImage";
import { Github, Linkedin, Mail, Twitter } from "lucide-react";

const teamMembers = [
  {
    name: "Siggi Simonarson",
    email: "siggi@buildbuddy.io",
    github: "siggisim",
    twitter: "siggi",
    linkedin: "siggisim",
    blurb: "Previously Senior Software Engineer at Google — 6 years. Studied Computer Science at Virginia Tech.",
    image: "siggi.jpg",
  },
  {
    name: "Tyler Williams",
    email: "tyler@buildbuddy.io",
    github: "tylerwilliams",
    twitter: "tylerbw",
    linkedin: "tyler-williams-80480519b",
    blurb:
      "Previously Staff Software Engineer at Google — 7 years. Studied Electrical Engineering & Computer Science at MIT.",
    image: "tyler.jpg",
  },
  {
    name: "George Li",
    email: "george@buildbuddy.io",
    github: "gtli7",
    twitter: "gtli_",
    linkedin: "gli",
    blurb:
      "Previously Head of APAC Sales Engineering at Looker (acquired by Google Cloud). Studied Computer Science at UVA.",
    image: "george.jpg",
  },
  {
    name: "Brandon Duffany",
    email: "brandon@buildbuddy.io",
    github: "bduffany",
    twitter: "brandon_duffany",
    linkedin: "brandon-duffany-39b7217a",
    blurb: "Previously Software Engineer at Google. Studied Computer Science at Cornell.",
    image: "brandon.jpg",
  },
  {
    name: "Vadim Berezniker",
    email: "vadim@buildbuddy.io",
    github: "vadimberezniker",
    linkedin: "vadimberezniker",
    blurb: "Previously Senior Software Engineer at Google — 7 years. Studied Computer Science at Stony Brook.",
    image: "vadim.png",
  },
  {
    name: "Zoey Greer",
    email: "zoey@buildbuddy.io",
    github: "tempoz",
    linkedin: "zoey-greer",
    blurb: "Previously Software Engineer at Google. Studied Computer Science at Virginia Tech.",
    image: "zoey.png",
  },
  {
    name: "Lulu Zhang",
    email: "lulu@buildbuddy.io",
    github: "luluz66",
    linkedin: "luluzhang66",
    blurb:
      "Previously Senior Software Engineer at Google & Thumbtack. Studied Computer Science at University of California, Irvine.",
    image: "lulu.jpg",
  },
  {
    name: "Brentley Jones",
    email: "brentley@buildbuddy.io",
    github: "brentleyjones",
    twitter: "brentleyjones",
    linkedin: "brentleyjones",
    blurb: "Previously Staff Software Engineer at Lyft & Target. Maintainer of Bazel's rules_apple and rules_swift.",
    image: "brentley.jpg",
  },
  {
    name: "Maggie Lou",
    email: "maggie@buildbuddy.io",
    github: "maggie-lou",
    linkedin: "maggie-lou-4a3bb110a",
    blurb: "Previously Software Engineer at Thumbtack. Studied Computer Science at Northwestern.",
    image: "maggie.jpeg",
  },
  {
    name: "Iain Macdonald",
    email: "iain@buildbuddy.io",
    github: "iain-macdonald",
    linkedin: "macdonaldi",
    blurb: "Previously Senior Software Engineer at Google — 10 years. Studied Software Engineering at McGill.",
    image: "iain.jpg",
  },
  {
    name: "Jim Hollenbach",
    email: "jim@buildbuddy.io",
    github: "jdhollen",
    twitter: "jdhollen",
    linkedin: "jim-hollenbach-5a49659",
    blurb: "Previously Staff Software Engineer at Google — 10 years. Studied Computer Science at MIT.",
    image: "jim.jpg",
  },
  {
    name: "Son Luong Ngoc",
    email: "son@buildbuddy.io",
    github: "sluongng",
    twitter: "sluongng",
    linkedin: "luongngocson",
    blurb: "Previously Senior DevOps Engineer & SRE at Qarik, Booking.com, Lazada & Alibaba.",
    image: "son.jpg",
  },
  {
    name: "Grace Chuang",
    email: "grace@buildbuddy.io",
    linkedin: "grace2chuang",
    blurb:
      "Previously Finance, Operations, and Product Strategy at Google & J.P. Morgan. Studied Commerce, Finance and Marketing at UVA.",
    image: "grace.jpg",
  },
];

function Team() {
  return (
    <Layout title="Team">
      <div className={common.page}>
        <div className={common.section}>
          <div className={common.container}>
            <div className={common.centeredText}>
              <div className={common.title}>We're deeply passionate about making developers more productive.</div>
              <div className={common.subtitle}>
                <br />
                <span className={styles.subtitleHeading}>We're based out of San Francisco, California. </span>
                Prior to starting BuildBuddy — we collectively spent over a decade at Google building products across
                Android, Google Maps, Search, Google Assistant, Google Cloud, AdWords, and Area 120.
              </div>
            </div>
          </div>
        </div>
        <div className={common.container}>
          <div className={styles.team}>
            {shuffle(teamMembers).map((teamMember) => (
              <div className={styles.teamMember} key={teamMember.name}>
                <div className={styles.teamMemberImage}>
                  <Image
                    img={require(`../../static/img/team/${teamMember.image}`)}
                    shouldAutoDownload={() => true}
                    threshold={10000}
                  />
                </div>
                <div className={styles.teamMemberHeading}>
                  <div className={styles.teamMemberName}>{teamMember.name}</div>

                  <div className={styles.teamMemberContact}>
                    {teamMember.email && (
                      <a target="_blank" href={`mailto:${teamMember.email}`}>
                        <Mail />
                      </a>
                    )}
                    {teamMember.github && (
                      <a target="_blank" href={`https://github.com/${teamMember.github}`}>
                        <Github />
                      </a>
                    )}
                    {teamMember.twitter && (
                      <a target="_blank" href={`https://twitter.com/${teamMember.twitter}`}>
                        <Twitter />
                      </a>
                    )}
                    {teamMember.linkedin && (
                      <a target="_blank" href={`https://www.linkedin.com/in/${teamMember.linkedin}`}>
                        <Linkedin />
                      </a>
                    )}
                  </div>
                </div>
                <div className={styles.teamMemberBlurb}>{teamMember.blurb}</div>
              </div>
            ))}
          </div>
        </div>

        <div className={`${common.section} ${common.sectionGray}`}>
          <div className={common.container}>
            <div className={`${styles.text}`}>
              <h2 className={styles.title}>Our investors</h2>
              <div className={styles.investors}>
                <div>
                  <a href="https://ycombinator.com" target="_blank">
                    <img alt="Y Combinator" src="/img/ycombinator-logo.png" />
                  </a>
                </div>
                <div>
                  <a href="https://addition.com" target="_blank">
                    <img alt="Addition" src="/img/addition-logo.svg" />
                  </a>
                </div>
                <div>
                  <a href="https://villageglobal.vc" target="_blank">
                    <img alt="Village Global" src="/img/vg-logo.png" />
                  </a>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className={common.section}>
          <div className={common.container}>
            <div className={`${common.centeredText}`}>
              <h2 className={common.title}>Interested in joining our growing team?</h2>
              <a href="/careers" className={`${common.button} ${common.buttonPrimary} ${styles.button}`}>
                View open positions
              </a>
              <Image
                style={{ backgroundColor: "#202124", padding: "8px", borderRadius: "8px", marginTop: "32px" }}
                img={require(`../../static/img/team/team.png`)}
                shouldAutoDownload={() => true}
                threshold={10000}
              />
            </div>
          </div>
        </div>
      </div>
    </Layout>
  );
}

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

export default Team;
