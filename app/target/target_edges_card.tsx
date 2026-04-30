import { ChevronDown, ChevronRight } from "lucide-react";
import React from "react";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { TextLink } from "../components/link/link";
import InvocationModel from "../invocation/invocation_model";
import router from "../router/router";
import {
  CompactExecLogActionSummary,
  CompactExecLogEdgesIndex,
  CompactExecLogTargetSummary,
  getCompactExecLogEdgesIndex,
} from "./compact_exec_log_edges";

interface Props {
  invocationId: string;
  model: InvocationModel;
  search: URLSearchParams;
  targetLabel: string;
  targetFiles: build_event_stream.File[];
}

interface State {
  loading: boolean;
  error?: string;
  index?: CompactExecLogEdgesIndex;
  expandedActionId?: string;
}

export default class TargetEdgesCardComponent extends React.Component<Props, State> {
  state: State = {
    loading: true,
  };

  private fetchRequestId = 0;
  private isComponentMounted = false;
  private actionHeaderRefs = new Map<string, HTMLDivElement>();

  componentDidMount() {
    this.isComponentMounted = true;
    this.fetchIndex();
  }

  componentWillUnmount() {
    this.isComponentMounted = false;
    this.fetchRequestId++;
  }

  componentDidUpdate(prevProps: Props, prevState: State) {
    const prevUri = prevProps.model.getExecutionLogFileUri();
    const nextUri = this.props.model.getExecutionLogFileUri();

    if (prevProps.model !== this.props.model || prevUri !== nextUri) {
      this.fetchIndex();
      return;
    }

    const prevExpandedActionId = prevProps.search.get("edgeAction");
    const nextExpandedActionId = this.props.search.get("edgeAction");
    if (prevProps.targetLabel !== this.props.targetLabel || prevExpandedActionId !== nextExpandedActionId) {
      this.setState({ expandedActionId: this.getExpandedActionId(this.state.index) });
    }

    if (
      prevProps.targetLabel !== this.props.targetLabel ||
      prevState.expandedActionId !== this.state.expandedActionId
    ) {
      this.scrollExpandedActionIntoView();
    }
  }

  private getExpandedActionId(index?: CompactExecLogEdgesIndex): string | undefined {
    const actionId = this.props.search.get("edgeAction");
    if (!index || !actionId) {
      return undefined;
    }
    if (!this.getResolvedCurrentTarget(index).actions.some((action) => action.id === actionId)) {
      return undefined;
    }
    return actionId;
  }

  private isStaleFetch(requestId: number, model: InvocationModel, executionLogUri: string | undefined): boolean {
    return (
      !this.isComponentMounted ||
      this.fetchRequestId !== requestId ||
      this.props.model !== model ||
      this.props.model.getExecutionLogFileUri() !== executionLogUri
    );
  }

  private fetchIndex() {
    const requestId = ++this.fetchRequestId;
    const model = this.props.model;
    const executionLogUri = model.getExecutionLogFileUri();
    if (!executionLogUri) {
      if (!this.isStaleFetch(requestId, model, executionLogUri)) {
        this.setState({ loading: false, error: undefined, index: undefined, expandedActionId: undefined });
      }
      return;
    }

    this.setState({ loading: true, error: undefined });
    getCompactExecLogEdgesIndex(executionLogUri, () => model.getExecutionLog())
      .then((index) => {
        if (this.isStaleFetch(requestId, model, executionLogUri)) {
          return;
        }
        this.setState({ index, loading: false, expandedActionId: this.getExpandedActionId(index) });
      })
      .catch((error) => {
        if (this.isStaleFetch(requestId, model, executionLogUri)) {
          return;
        }
        console.error("Failed to load compact execution log edges", error);
        this.setState({
          loading: false,
          error: error instanceof Error ? error.message : "Failed to load compact execution log edges.",
          expandedActionId: undefined,
        });
      });
  }

  private getCurrentTarget(): CompactExecLogTargetSummary | undefined {
    return this.state.index?.targets[this.props.targetLabel];
  }

  private getTargetFilePaths(): string[] {
    return this.props.targetFiles.map(getBuildEventFilePath).filter((path): path is string => Boolean(path));
  }

  private getFallbackTargetActions(index?: CompactExecLogEdgesIndex): CompactExecLogActionSummary[] {
    if (!index) {
      return [];
    }

    const filePaths = new Set(this.getTargetFilePaths());
    if (!filePaths.size) {
      return [];
    }

    return Object.values(index.actions)
      .filter((action) => action.primaryOutputPath && filePaths.has(action.primaryOutputPath))
      .sort(compareActionSummaries);
  }

  private getResolvedCurrentTarget(index = this.state.index): {
    actions: CompactExecLogActionSummary[];
    effectiveTargetLabel?: string;
  } {
    const currentTarget = index?.targets[this.props.targetLabel];
    if (currentTarget) {
      return {
        actions: this.getCurrentTargetActions(currentTarget, index),
        effectiveTargetLabel: currentTarget.label,
      };
    }

    const fallbackActions = this.getFallbackTargetActions(index);
    const effectiveTargetLabel =
      fallbackActions.length && fallbackActions.every((action) => action.targetLabel === fallbackActions[0].targetLabel)
        ? fallbackActions[0].targetLabel
        : undefined;
    return { actions: fallbackActions, effectiveTargetLabel };
  }

  private getAction(actionId: string): CompactExecLogActionSummary | undefined {
    return this.state.index?.actions[actionId];
  }

  private getCurrentTargetActions(
    currentTarget: CompactExecLogTargetSummary,
    index = this.state.index
  ): CompactExecLogActionSummary[] {
    return currentTarget.actionIds
      .map((actionId) => index?.actions[actionId])
      .filter((action): action is CompactExecLogActionSummary => Boolean(action))
      .sort(compareActionSummaries);
  }

  private getRelatedActions(
    action: CompactExecLogActionSummary,
    direction: "parents" | "children"
  ): CompactExecLogActionSummary[] {
    const relations = direction === "parents" ? action.parents : action.children;
    const relatedActions = new Map<string, CompactExecLogActionSummary>();

    for (const relation of relations) {
      for (const relatedActionId of relation.actionIds) {
        const relatedAction = this.getAction(relatedActionId);
        if (!relatedAction) {
          continue;
        }
        relatedActions.set(relatedAction.id, relatedAction);
      }
    }

    return Array.from(relatedActions.values()).sort(compareRelatedActions);
  }

  private toggleExpandedAction(actionId: string) {
    this.setState((prevState) => ({
      expandedActionId: prevState.expandedActionId === actionId ? undefined : actionId,
    }));
  }

  private handleAccordionKeyDown = (event: React.KeyboardEvent<HTMLDivElement>, actionId: string) => {
    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }
    event.preventDefault();
    this.toggleExpandedAction(actionId);
  };

  private stopAccordionToggle = (event: React.MouseEvent<HTMLElement>) => {
    event.stopPropagation();
  };

  private setActionHeaderRef = (actionId: string, element: HTMLDivElement | null) => {
    if (!element) {
      this.actionHeaderRefs.delete(actionId);
      return;
    }
    this.actionHeaderRefs.set(actionId, element);
  };

  private scrollExpandedActionIntoView() {
    const expandedActionId = this.state.expandedActionId;
    if (!expandedActionId) {
      return;
    }

    const header = this.actionHeaderRefs.get(expandedActionId);
    if (!header) {
      return;
    }

    const { top, bottom } = header.getBoundingClientRect();
    if (top >= 0 && bottom <= window.innerHeight) {
      return;
    }

    header.scrollIntoView({ block: "nearest" });
  }

  private getActionHref(action: CompactExecLogActionSummary): string | undefined {
    if (!action.targetLabel) {
      return undefined;
    }
    const search = new URLSearchParams();
    search.set("target", action.targetLabel);
    search.set("edgeAction", action.id);
    return `${router.getInvocationUrl(this.props.invocationId)}?${search.toString()}#edges`;
  }

  private getTargetHref(targetLabel: string): string {
    return `${router.getInvocationUrl(this.props.invocationId)}?target=${encodeURIComponent(targetLabel)}#edges`;
  }

  private getActionText(action: CompactExecLogActionSummary): string {
    return action.cached ? `${action.label} (Cached)` : action.label;
  }

  private renderTargetLink(targetLabel: string) {
    const { effectiveTargetLabel } = this.getResolvedCurrentTarget();
    const truncatedTargetLabel = truncateTargetLabel(targetLabel);
    if (!targetLabel) {
      return <span className="target-edge-current-target target-edge-target-label">&lt;unknown target&gt;</span>;
    }
    if (targetLabel === effectiveTargetLabel || (!effectiveTargetLabel && targetLabel === this.props.targetLabel)) {
      return (
        <span className="target-edge-current-target target-edge-target-label" title={targetLabel}>
          {truncatedTargetLabel}
        </span>
      );
    }
    return (
      <TextLink
        className="target-edge-inline-link target-edge-target-label"
        href={this.getTargetHref(targetLabel)}
        title={targetLabel}
        onClick={this.stopAccordionToggle}>
        {truncatedTargetLabel}
      </TextLink>
    );
  }

  private renderActionLink(action: CompactExecLogActionSummary) {
    const label = this.getActionText(action);
    const href = this.getActionHref(action);
    if (!href) {
      return (
        <span
          className="target-edge-inline-link target-edge-action-link target-edge-inline-link-disabled"
          title={label}>
          {label}
        </span>
      );
    }
    return (
      <TextLink
        className="target-edge-inline-link target-edge-action-link"
        href={href}
        onClick={this.stopAccordionToggle}
        title={label}>
        {label}
      </TextLink>
    );
  }

  private renderCombinedActionLabel(action: CompactExecLogActionSummary) {
    const actionStatusClass = getActionStatusClass(action);
    return (
      <span className="target-edge-action-summary">
        <span className={`target-edge-result-indicator ${actionStatusClass}`} />
        <span className="target-edge-combined-label">
          {this.renderTargetLink(action.targetLabel)}
          <span className="target-edge-arrow">{" -> "}</span>
          {this.renderActionLink(action)}
        </span>
      </span>
    );
  }

  private renderRelatedActionsSection(
    title: string,
    actions: CompactExecLogActionSummary[],
    emptyState: string
  ): React.ReactNode {
    return (
      <div className="target-edges-accordion-section">
        <div className="target-edges-accordion-section-title">{title}</div>
        {actions.length ? (
          <div className="target-edges-related-actions">
            {actions.map((action) => (
              <div className="target-edges-related-action" key={action.id}>
                {this.renderCombinedActionLabel(action)}
              </div>
            ))}
          </div>
        ) : (
          <div className="target-edges-empty-state">{emptyState}</div>
        )}
      </div>
    );
  }

  private renderActionAccordion(action: CompactExecLogActionSummary): React.ReactNode {
    const expanded = this.state.expandedActionId === action.id;
    const parents = this.getRelatedActions(action, "parents");
    const children = this.getRelatedActions(action, "children");
    const Chevron = expanded ? ChevronDown : ChevronRight;
    const actionStatusClass = getActionStatusClass(action);

    return (
      <div className={`target-edges-accordion-item ${expanded ? "expanded" : ""}`} key={action.id}>
        <div
          className={`target-edges-accordion-header ${actionStatusClass}`}
          onClick={() => this.toggleExpandedAction(action.id)}
          onKeyDown={(event) => this.handleAccordionKeyDown(event, action.id)}
          ref={(element) => this.setActionHeaderRef(action.id, element)}
          role="button"
          tabIndex={0}>
          <span className="target-edges-accordion-chevron">
            <Chevron className="icon" />
          </span>
          <span className="target-edges-accordion-label">{this.renderCombinedActionLabel(action)}</span>
        </div>
        {expanded && (
          <div className="target-edges-accordion-body">
            <div className="target-edges-accordion-sections">
              {this.renderRelatedActionsSection("Parent actions", parents, "No upstream actions found.")}
              {this.renderRelatedActionsSection("Child actions", children, "No downstream actions found.")}
            </div>
          </div>
        )}
      </div>
    );
  }

  render() {
    if (this.state.loading) {
      return <div className="loading loading-slim invocation-tab-loading" />;
    }

    if (this.state.error) {
      return <div className="invocation-execution-empty-state">{this.state.error}</div>;
    }

    if (!this.props.model.hasExecutionLog()) {
      return (
        <div className="invocation-execution-empty-state">No compact execution log found for this invocation.</div>
      );
    }

    const currentTarget = this.getCurrentTarget();
    const { actions: currentTargetActions } = this.getResolvedCurrentTarget();

    return (
      <div className="card expanded target-edges-card">
        <div className="content">
          <div className="invocation-content-header target-edges-header">
            <div>
              <div className="title">Build graph edges</div>
              <div className="target-edges-subtitle">
                Expand an action to inspect its parent and child actions from the compact execution log.
              </div>
            </div>
          </div>

          {!currentTarget && !currentTargetActions.length && (
            <div className="invocation-execution-empty-state">
              No compact execution log edges were recorded for {this.props.targetLabel}.
            </div>
          )}

          {currentTarget && !currentTargetActions.length && (
            <div className="target-edges-empty-state">No actions were recorded for this target.</div>
          )}

          {currentTargetActions.length > 0 && (
            <div className="target-edges-accordion">
              {currentTargetActions.map((action) => this.renderActionAccordion(action))}
            </div>
          )}
        </div>
      </div>
    );
  }
}

function compareLabels(a: string, b: string): number {
  const bucket = (s: string) => (s.startsWith("//") ? 0 : s.startsWith("@") ? 1 : 2);
  const bucketA = bucket(a);
  const bucketB = bucket(b);
  if (bucketA !== bucketB) {
    return bucketA - bucketB;
  }
  return a.localeCompare(b);
}

function compareRelatedActions(a: CompactExecLogActionSummary, b: CompactExecLogActionSummary): number {
  const targetCmp = compareLabels(a.targetLabel || "", b.targetLabel || "");
  if (targetCmp !== 0) {
    return targetCmp;
  }
  return compareActionSummaries(a, b);
}

function compareActionSummaries(a: CompactExecLogActionSummary, b: CompactExecLogActionSummary): number {
  if (a.cached !== b.cached) {
    return Number(a.cached) - Number(b.cached);
  }
  const mnemonicCmp = (a.mnemonic || "").localeCompare(b.mnemonic || "");
  if (mnemonicCmp !== 0) {
    return mnemonicCmp;
  }
  const labelCmp = (a.label || "").localeCompare(b.label || "");
  if (labelCmp !== 0) {
    return labelCmp;
  }
  return a.id.localeCompare(b.id);
}

function getBuildEventFilePath(file: build_event_stream.File): string | undefined {
  const components = [...(file.pathPrefix || []), file.name || ""].filter(Boolean);
  return components.length ? components.join("/") : undefined;
}

function getActionStatusClass(action: CompactExecLogActionSummary): "cached" | "failure" | "success" {
  if (action.result === "failed") {
    return "failure";
  }
  return action.cached ? "cached" : "success";
}

const MAX_TARGET_LABEL_LENGTH = 48;

function truncateTargetLabel(targetLabel: string): string {
  if (targetLabel.length <= MAX_TARGET_LABEL_LENGTH) {
    return targetLabel;
  }

  const targetNameIndex = targetLabel.lastIndexOf(":");
  const suffix = targetNameIndex >= 0 ? targetLabel.slice(targetNameIndex) : "";
  const prefixBudget = MAX_TARGET_LABEL_LENGTH - suffix.length - 3;
  if (prefixBudget <= 8) {
    return `${targetLabel.slice(0, MAX_TARGET_LABEL_LENGTH - 3)}...`;
  }
  return `${targetLabel.slice(0, prefixBudget)}...${suffix}`;
}
