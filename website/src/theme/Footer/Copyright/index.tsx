import Link from "@docusaurus/Link";
import OriginalCopyright from "@theme-original/Footer/Copyright";
import type { Props } from "@theme/Footer/Copyright";
import { Linkedin, Slack, Twitter } from "lucide-react";
import React from "react";
import { GithubIcon } from "../../../../../app/icons/github";

const socialLinks = [
  { label: "Slack", href: "http://community.buildbuddy.io/", Icon: Slack },
  { label: "Twitter", href: "https://twitter.com/buildbuddy", Icon: Twitter },
  { label: "LinkedIn", href: "http://linkedin.com/company/buildbuddy", Icon: Linkedin },
  { label: "GitHub", href: "https://github.com/buildbuddy-io", Icon: GithubIcon },
];

export default function FooterCopyright(props: Props) {
  return (
    <div className="footer-bottom-row">
      <div className="footer-legal">
        <OriginalCopyright {...props} />
        <nav className="footer-legal-links" aria-label="Legal">
          <Link to="/privacy">Privacy</Link>
          <Link to="/terms">Terms</Link>
        </nav>
      </div>
      <nav className="footer-social-links" aria-label="Connect with BuildBuddy">
        {socialLinks.map(({ label, href, Icon }) => (
          <a key={label} href={href} target="_blank" rel="noopener noreferrer" aria-label={label} title={label}>
            <span aria-hidden="true">
              <Icon className="footer-social-icon" />
            </span>
          </a>
        ))}
      </nav>
    </div>
  );
}
