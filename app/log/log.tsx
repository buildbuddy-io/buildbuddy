import React from "react";
import parseAnsi from "../terminal/ansi";
import { RowSpan } from "../terminal/row";
import { CheckCircle, ChevronDown, ChevronRight, PlayCircle, XCircle } from "lucide-react";
import { durationUsec } from "../format/format";
import { invocation } from "../../proto/invocation_ts_proto";
import rpc_service from "../service/rpc_service";
import InvocationCardComponent from "../invocation/invocation_card";

export interface LogProps {
  /**
   * Text to be displayed in the log.
   * This will be parsed into sections.
   */
  text: string;
}

interface LogState {
  childInvocations: Map<string, invocation.Invocation>;
}

interface Section {
  header: string;
  contents: string;

  childInvocationIds?: string[];

  ok?: boolean;
  timestampUsec?: number;
  durationUsec?: number;
}

export default class Log extends React.Component<LogProps, LogState> {
  state: LogState = {
    childInvocations: new Map(),
  };

  componentDidMount() {
    this.fetchChildInvocations();
  }

  componentDidUpdate(prevProps: LogProps) {
    if (this.props.text !== prevProps.text) {
      this.fetchChildInvocations();
    }
  }

  private fetchChildInvocations() {
    const sections = parseSections(this.props.text);
    const invocationIds = sections.flatMap((section) => section.childInvocationIds ?? []);
    if (!invocationIds.length) return;

    rpc_service.service
      .searchInvocation({
        query: new invocation.InvocationQuery({
          invocationIds,
        }),
      })
      .then((response) => {
        const childInvocations = new Map<string, invocation.Invocation>();
        for (const child of response.invocation) {
          childInvocations.set(child.invocationId, child);
        }
        this.setState({ childInvocations });
      });
  }

  render() {
    console.log("Text\n", this.props.text);
    const sections = parseSections(this.props.text);
    console.debug("sections", sections);

    return (
      <div className="log">
        {sections.map((section, i) => (
          <LogSection
            key={i}
            section={section}
            isCurrent={i === sections.length - 1}
            childInvocations={this.state.childInvocations}
          />
        ))}
      </div>
    );
  }
}

interface LogSectionProps {
  section: Section;
  isCurrent: boolean;
  childInvocations: Map<string, invocation.Invocation>;
}

interface LogSectionState {
  expanded: boolean | null;
}

class LogSection extends React.Component<LogSectionProps, LogSectionState> {
  state: LogSectionState = {
    expanded: null,
  };

  private onClickHeader() {
    this.setState({ expanded: !this.isExpanded() });
  }

  private isExpanded() {
    return this.state.expanded !== null ? this.state.expanded : this.props.isCurrent && !this.props.section.ok;
  }

  render() {
    const section = this.props.section;
    return (
      <div className={`log-section ${this.isExpanded() ? "expanded" : ""} dark-mode`}>
        <div className="header" onClick={() => this.onClickHeader()}>
          {this.isExpanded() ? <ChevronDown className="icon white" /> : <ChevronRight className="icon white" />}
          {section.ok === undefined ? (
            <PlayCircle className="icon blue" />
          ) : section.ok ? (
            <CheckCircle className="icon green" />
          ) : (
            <XCircle className="icon red" />
          )}
          <span className="header-text">{section.header}</span>
          {/* TODO: show time since start timestamp */}
          <span className="duration">{section.durationUsec ? durationUsec(section.durationUsec) : null}</span>
        </div>
        {/* TODO: clean up ".terminal" classes */}
        {this.isExpanded() && (
          <div className="terminal">
            {section.contents.split("\n").map((line, i) => {
              const spans = parseAnsi(line);
              return (
                <div className="terminal-line">
                  {spans.map((part, i) => (
                    <RowSpan key={i} text={part.text} style={part.style} isActiveMatch={false} matchIndex={null} />
                  ))}
                </div>
              );
            })}
          </div>
        )}
        {section.childInvocationIds && (
          <div className="history">
            {section.childInvocationIds?.map((id) => {
              const invocation = this.props.childInvocations.get(id);
              if (!invocation) return null;
              return <InvocationCardComponent invocation={invocation} />;
            })}
          </div>
        )}
      </div>
    );
  }
}

const INVOCATION_REGEX = /Streaming build results to: https?:\/\/.*\/(?<id>[A-Za-z0-9\-]+)$/;

function parseSections(text: string): Section[] {
  const sections: Section[] = [];
  let section: Section | null = null;
  for (const line of text.split("\n")) {
    // Parse section annotations (log lines starting with @buildbuddy)
    if (line.startsWith("@buildbuddy")) {
      const data = JSON.parse(line.substring("@buildbuddy".length)) as Record<string, any>;
      if ("section" in data) {
        section = { header: data["section"], contents: "", timestampUsec: data["timestampUsec"] };
        sections.push(section);
      } else if ("durationUsec" in data) {
        section ??= { header: "Workflow step", contents: "" };
        section.ok = data["ok"];
        section.durationUsec = data["durationUsec"];
      }
      continue;
    }
    if (!section) {
      section = { header: "Setup", contents: "" };
      sections.push(section);
    }
    const match = line.match(INVOCATION_REGEX);
    if (match) {
      const invocationID = match.groups?.["id"] ?? "";
      section.childInvocationIds ??= [];
      if (invocationID && !section.childInvocationIds.includes(invocationID)) {
        section.childInvocationIds.push(invocationID);
      }
    }
    section.contents += line + "\n";
  }
  return sections;
}
