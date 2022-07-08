import React from "react";
import Layout from "@theme/Layout";
import common from "../css/common.module.css";
import styles from "./careers.module.css";
import { ArrowRight } from "lucide-react";

const careers = [
  { name: "Software Engineer" },
  { name: "Senior Software Engineer" },
  { name: "Senior Site Reliability Engineer" },
  { name: "Solutions Engineer" },
];

const globalSpace = new RegExp(" ", "g");

function Careers() {
  return (
    <Layout title="Careers">
      <div className={common.page}>
        <div className={common.section}>
          <div className={common.container}>
            <div className={styles.jobs}>
              <div className={common.title}>Current Openings</div>
              {careers.map((career) => (
                <a
                  href={`/jobs/${career.name.toLowerCase().replace(globalSpace, "-")}`}
                  className={styles.job}
                  key={career.name}>
                  <div>
                    <div className={styles.jobName}>{career.name}</div>
                    <div className={styles.jobLocation}>Full-Time, San Francisco Bay Area & Remote</div>
                  </div>
                  <ArrowRight />
                </a>
              ))}
            </div>
          </div>
        </div>
      </div>
    </Layout>
  );
}

export default Careers;
