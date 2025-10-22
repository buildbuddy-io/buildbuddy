import Heading from "@theme/Heading";
import Image from "@theme/IdealImage";
import type { Props } from "@theme/NotFound/Content";
import clsx from "clsx";
import React from "react";

export default function NotFoundContent({ className }: Props): JSX.Element {
  return (
    <main className={clsx("container margin-vert--xl", className)}>
      <div className="row row--offset-2">
        <div className="col col--3 col--offset-2">
          <Image img={require("../../../../static/img/buddy-clay.png")} />
        </div>
        <div className="col col--6">
          <Heading as="h1" className="hero__title">
            404 &mdash; Oops, wrong target.
          </Heading>
          <p>Looks like this page wasn't in our build graph.</p>
          <p>
            Don't worry, you can:
            <ul>
              <li>
                <u>
                  <a href="/">Go home</a>
                </u>
              </li>
              <li>
                <u>
                  <a href="/docs/introduction/">Browse docs</a>
                </u>
              </li>
              <li>
                <u>
                  <a href="https://github.com/buildbuddy-io/buildbuddy">View on GitHub</a>
                </u>
              </li>
            </ul>
          </p>
        </div>
      </div>
    </main>
  );
}
