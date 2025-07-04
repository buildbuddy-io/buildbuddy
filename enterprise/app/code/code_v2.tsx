import * as diff from "diff";
import { ArrowLeft, ChevronRight, Download, Key, Pencil, Send, XCircle } from "lucide-react";
import * as monaco from "monaco-editor";
import React from "react";
import { Subscription } from "rxjs";
import alert_service from "../../../app/alert/alert_service";
import { User } from "../../../app/auth/auth_service";
import { FilledButton, OutlinedButton } from "../../../app/components/button/button";
import Spinner from "../../../app/components/spinner/spinner";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { git } from "../../../proto/git_ts_proto";
import { runner } from "../../../proto/runner_ts_proto";
import CodeBuildButton from "./code_build_button";
import CodeEmptyStateComponent from "./code_empty";
import { createPullRequest, updatePullRequest } from "./code_pull_request";
import SidebarNodeComponentV2, { compareNodes } from "./code_sidebar_node_v2";

import Long from "long";
import capabilities from "../../../app/capabilities/capabilities";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Modal from "../../../app/components/modal/modal";
import SearchBar from "../../../app/components/search_bar/search_bar";
import error_service from "../../../app/errors/error_service";
import { GithubIcon } from "../../../app/icons/github";
import picker_service, { PickerModel } from "../../../app/picker/picker_service";
import router from "../../../app/router/router";
import { linkReadWriteGitHubAppURL } from "../../../app/util/github";
import { parseLcov } from "../../../app/util/lcov";
import { github } from "../../../proto/github_ts_proto";
import * as kythe_common from "../../../proto/kythe_common_ts_proto";
import { kythe } from "../../../proto/kythe_xref_ts_proto";
import { build } from "../../../proto/remote_execution_ts_proto";
import { search } from "../../../proto/search_ts_proto";
import { workspace } from "../../../proto/workspace_ts_proto";
import { getLangHintFromFilePath } from "../monaco/monaco";
import OrgPicker from "../org_picker/org_picker";
import BazelrcSidekick from "../sidekick/bazelrc/bazelrc";
import BazelVersionSidekick from "../sidekick/bazelversion/bazelversion";
import BuildFileSidekick from "../sidekick/buildfile/buildfile";
import ModuleSidekick from "../sidekick/module/module";

interface Props {
  user: User;
  tab: string;
  path: string;
  search: URLSearchParams;
}

interface State {
  repoResponse: github.GetGithubRepoResponse | undefined;
  directoryResponse: workspace.GetWorkspaceDirectoryResponse | undefined;
  installationsResponse: github.GetGithubUserInstallationsResponse | undefined;

  fullPathToExpanded: Map<string, boolean>;
  fullPathToRenaming: Map<string, boolean>;
  fullPathToChildrenMap: Map<string, workspace.Node[]>;
  fullPathToIncludeChanges: Map<string, boolean>;

  fullPathToNodeMap: Map<string, workspace.Node>;
  fullPathToModelMap: Map<string, monaco.editor.ITextModel>;
  fullPathToDecorationsMap: Map<string, string[]>;
  fullPathToDiffModelMap: Map<string, monaco.editor.IDiffEditorModel>;

  originalFileContents: Map<string, string>;

  tabs: Map<string, string>;
  changes: Map<string, workspace.Node>;
  temporaryFiles: Map<string, workspace.Node[]>;
  mergeConflicts: Map<string, string>;

  prLink: string;
  prNumber: Long;
  prBranch: string;
  prTitle: string;
  prBody: string;

  loading: boolean;

  requestingReview: boolean;
  updatingPR: boolean;
  reviewRequestModalVisible: boolean;
  isBuilding: boolean;

  showContextMenu?: boolean;
  contextMenuX?: number;
  contextMenuY?: number;
  contextMenuFullPath?: string;
  contextMenuFile?: workspace.Node;

  commands: string[];
  defaultConfig: string;

  xrefsLoading: boolean;
  extendedXrefs?: search.ExtendedXrefsReply;
  xrefsHeight: number;
}

// When upgrading monaco, make sure to run
// cp node_modules/monaco-editor/min/vs/editor/editor.main.css enterprise/app/code/monaco.css
// and replace ../base/browser/ui/codicons/codicon/codicon.ttf
// with https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.47.0/min/vs/base/browser/ui/codicons/codicon/codicon.ttf
const MONACO_VERSION = "0.47.0";
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export default class CodeComponentV2 extends React.Component<Props, State> {
  state: State = {
    repoResponse: undefined,
    directoryResponse: undefined,
    installationsResponse: undefined,
    fullPathToExpanded: new Map<string, boolean>(),
    fullPathToRenaming: new Map<string, boolean>(),
    fullPathToChildrenMap: new Map<string, Array<workspace.Node>>(),
    fullPathToNodeMap: new Map<string, workspace.Node>(),
    fullPathToModelMap: new Map<string, monaco.editor.ITextModel>(),
    fullPathToDecorationsMap: new Map<string, string[]>(),
    fullPathToDiffModelMap: new Map<string, monaco.editor.IDiffEditorModel>(),
    originalFileContents: new Map<string, string>(),
    tabs: new Map<string, string>(),
    changes: new Map<string, workspace.Node>(),
    temporaryFiles: new Map<string, workspace.Node[]>(),
    mergeConflicts: new Map<string, string>(),
    fullPathToIncludeChanges: new Map<string, boolean>(),
    prLink: "",
    prBranch: "",
    prNumber: new Long(0),
    xrefsLoading: false,

    loading: false,

    requestingReview: false,
    updatingPR: false,
    reviewRequestModalVisible: false,
    isBuilding: false,

    prTitle: "",
    prBody: "",

    commands: ["build //...", "test //..."],
    defaultConfig: "",
    xrefsHeight: 300,
  };

  editor: monaco.editor.IStandaloneCodeEditor | undefined;
  diffEditor: monaco.editor.IDiffEditor | undefined;

  // Note that these decoration collections are automatically cleared when the model is changed.
  searchDecorations: monaco.editor.IEditorDecorationsCollection | undefined;
  lcovDecorations: monaco.editor.IEditorDecorationsCollection | undefined;

  findRefsKey?: monaco.editor.IContextKey<boolean>;
  goToDefKey?: monaco.editor.IContextKey<boolean>;
  pendingXrefsRequest?: CancelablePromise<search.KytheResponse>;
  mousedownTarget?: monaco.Position;

  codeViewer = React.createRef<HTMLDivElement>();
  diffViewer = React.createRef<HTMLDivElement>();

  subscription?: Subscription;
  fetchedInitialContent = false;

  componentWillMount() {
    let githubUrl = this.props.search.get("github_url");
    if (githubUrl) {
      window.location.href = this.parseGithubUrl(githubUrl) + window.location.hash;
      return;
    }

    document.title = `Code | BuildBuddy`;

    this.fetchCode();

    this.subscription = rpcService.events.subscribe({
      next: (name) => name === "refresh" && this.fetchCode(),
    });
  }

  handleWindowResize() {
    this.editor?.layout();
    this.diffEditor?.layout();
  }

  needsGithubLink() {
    return this.currentRepo() && (!this.isSingleFile() || this.isLcov()) && !this.props.user.githubLinked;
  }

  isLcov() {
    return Boolean(this.props.search.get("lcov"));
  }

  isSingleFile() {
    return Boolean(this.props.search.get("bytestream_url")) || this.isLcov();
  }

  componentDidMount() {
    this.fetchInitialContent();

    document.addEventListener("keydown", this.onKeydown);
  }

  private onKeydown = (e: KeyboardEvent) => {
    switch (e.keyCode) {
      case 70: // Meta + F
        if (!e.metaKey) break;
        this.editor?.focus();
        this.editor?.trigger("find", "editor.actions.findWithArgs", { searchString: "" });
        e.preventDefault();
        break;
      case 80: // Meta + P
        if (!e.metaKey) break;
        e.preventDefault();
        this.showFileSearch();
    }
  };

  showFileSearch() {
    let picker: PickerModel = {
      title: "Search",
      placeholder: "",
      // TODO: add debounce support
    };
    if (capabilities.config.codeSearchEnabled) {
      picker.fetchOptions = async (query) => {
        // TODO: include repo info once supported
        // TODO: kick off indexing on page load
        return (
          await rpcService.service.search(
            new search.SearchRequest({ query: new search.Query({ term: `filepath:${query}` }) })
          )
        ).results.map((r) => r.filename);
      };
    } else {
      picker.options = this.state.directoryResponse?.childNodes.map((n) => n.path) || [];
    }
    picker_service.show(picker);
    picker_service.picked.subscribe((path) => {
      if (!path) {
        return;
      }
      this.fetchIfNeededAndNavigate(path);
    });
  }

  getDisplayOptions(o: any, className: string = "code-hover"): monaco.editor.IModelDecorationOptions | null {
    const allowedKinds = [
      "/kythe/edge/ref/call",
      "/kythe/edge/ref/imports",
      "/kythe/edge/defines/binding",
      "/kythe/edge/ref",
      "/kythe/edge/ref/writes",
    ];
    if (!allowedKinds.includes(o.kind)) {
      return null;
    }
    return { inlineClassName: className, hoverMessage: { value: o.target_ticket } };
  }

  ticketsForPosition(pos: monaco.Position): string[] {
    const refs = this.getKytheRefsForRange(new monaco.Range(pos.lineNumber, pos.column, pos.lineNumber, pos.column));
    if (!refs.length) {
      return [];
    }
    return refs.map((ref) => ref.targetTicket).filter((ticket) => ticket);
  }

  navigateToDefinitionOrPopulatePanel(pos: monaco.Position) {
    const refs = this.getKytheRefsForRange(new monaco.Range(pos.lineNumber, pos.column, pos.lineNumber, pos.column));
    if (!refs.length) {
      // clear all highlights when clicking on a non-reference
      this.updateSymbolHighlights(this.editor?.getModel()!, []);
      return;
    }

    const tickets = refs.map((ref) => ref.targetTicket).filter((ticket) => ticket);
    const isDef = refs.reduce((acc, ref) => acc || ref.kind === "/kythe/edge/defines/binding", false);

    if (isDef) {
      this.populateXrefsPanel(tickets);
    } else {
      this.navigateToDefinition(tickets, true);
    }
  }

  navigateToDefinition(tickets: string[], fallbackToPanel = false) {
    if (!tickets?.length) {
      return;
    }
    tickets = [...new Set(tickets)]; // Remove duplicates

    const kytheReq = new search.KytheRequest({
      crossReferencesRequest: new kythe.proto.CrossReferencesRequest({
        snippets: kythe.proto.SnippetsKind.DEFAULT,
        ticket: tickets,
        declarationKind: kythe.proto.CrossReferencesRequest.DeclarationKind.NO_DECLARATIONS,
        referenceKind: kythe.proto.CrossReferencesRequest.ReferenceKind.NO_REFERENCES,
        definitionKind: kythe.proto.CrossReferencesRequest.DefinitionKind.BINDING_DEFINITIONS,
      }),
    });

    this.fetchKytheData(kytheReq)
      .then((kytheReply) => {
        let xrefReply = kytheReply.crossReferencesReply;
        if (!xrefReply) {
          console.log("Warning: No xrefs found for tickets", tickets);
          return;
        }

        const defs = Object.values(xrefReply.crossReferences).filter((item) => item.definition.length > 0);

        let anchor: kythe.proto.Anchor | undefined = undefined;
        if (defs.length > 0 && defs[0].definition && defs[0].definition.length > 0 && defs[0].definition[0].anchor) {
          anchor = defs[0].definition[0].anchor;
        }

        if (!anchor) {
          if (fallbackToPanel) {
            this.populateXrefsPanel(tickets);
          } else {
            console.log("Warning: No definitions found for tickets", tickets);
          }
        } else {
          this.navigateToAnchor(anchor);
        }
      })
      .catch((e) => console.log("Error fetching kythe data", e));
  }

  populateXrefsPanel(tickets: string[]) {
    if (!tickets || tickets.length === 0) {
      return;
    }

    tickets = [...new Set(tickets)]; // Remove duplicates
    const xrefsReq = new search.KytheRequest({
      extendedXrefsRequest: new search.ExtendedXrefsRequest({
        tickets: tickets,
      }),
    });

    this.fetchKytheData(xrefsReq)
      .then((kytheReply) => {
        if (!kytheReply) {
          return;
        }

        let xrefsReply = kytheReply.extendedXrefsReply;
        if (!xrefsReply) {
          console.log("Warning: No extendedXrefs found for tickets", tickets);
          return;
        }
        this.setState({ extendedXrefs: xrefsReply });
      })
      .catch((e) => console.log("Error fetching kythe data", e));
  }

  clearHighlights(model: monaco.editor.ITextModel) {
    let modDecs: monaco.editor.IModelDecoration[] = [];

    model.getAllDecorations().forEach((decor: monaco.editor.IModelDecoration) => {
      const ref = this.decorToReference(decor);
      if (!ref) {
        return;
      }

      if (decor.options.inlineClassName !== "code-hover") {
        decor.options.inlineClassName = "code-hover";
        modDecs.push(decor);
      }
    });
    if (modDecs.length > 0) {
      model.deltaDecorations(
        modDecs.map((x) => x.id),
        modDecs
      );
    }
  }

  updateSymbolHighlights(model: monaco.editor.ITextModel, tickets: string[]) {
    let modDecs: monaco.editor.IModelDecoration[] = [];

    // Go through each decoration. Find decorations with matching tickets. Update their class names
    // to highlight them. At the same time, remove non-matching highlights.
    model.getAllDecorations().forEach((decor: monaco.editor.IModelDecoration) => {
      const ref = this.decorToReference(decor);
      if (!ref) {
        return;
      }

      let newClassName = "";
      if (tickets.includes(ref.targetTicket)) {
        // This decoration matches the hovered ticket - highlight it if the edge type warrants it.
        if (ref.kind === "/kythe/edge/ref") {
          newClassName = "code-highlight-reference";
        } else if (ref.kind === "/kythe/edge/defines/binding" || ref.kind === "/kythe/edge/ref/writes") {
          newClassName = "code-highlight-modification";
        }
      } else if (decor.options.inlineClassName !== "code-hover") {
        // reset no-longer-matching nodes
        newClassName = "code-hover";
      }

      if (newClassName) {
        decor.options.inlineClassName = newClassName;
        modDecs.push(decor);
      }
    });

    if (modDecs.length > 0) {
      model.deltaDecorations(
        modDecs.map((x) => x.id),
        modDecs
      );
    }
  }

  // Translates node facts /node/kind and /subkind into a human-readable description.
  // See https://kythe.io/docs/schema/#_node_kinds
  nodeInfoToMarkdownDescription(nodeInfo: kythe_common.kythe.proto.common.NodeInfo | null | undefined): string {
    if (!nodeInfo?.facts || !("/kythe/node/kind" in nodeInfo.facts)) {
      return "";
    }

    const kind = textDecoder.decode(nodeInfo.facts["/kythe/node/kind"]);
    const subkind = textDecoder.decode(nodeInfo.facts["/kythe/subkind"]);
    let description = "";

    switch (kind) {
      case "function":
        switch (subkind) {
          case "constructor":
            description = "Constructor";
            break;
          case "destructor":
            description = "Destructor";
            break;
          default:
            description = "Function";
            break;
        }
        break;
      case "variable":
        switch (subkind) {
          case "local":
          case "local/exception":
          case "local/resource":
            description = "Local Variable";
            break;
          case "local/parameter":
            description = "Parameter";
            break;
          case "field":
            description = "Field";
            break;
          case "import":
            description = "Imported Variable";
            break;
          default:
            description = "Variable";
            break;
        }
        break;
      case "record":
        switch (subkind) {
          case "class":
            description = "Class";
            break;
          case "enum":
          case "enumClass":
            description = "Enum";
            break;
          case "struct":
            description = "Struct";
            break;
          case "union":
            description = "Union";
            break;
          case "type":
            description = "Type";
            break;
          default:
            description = "Record";
            break;
        }
        break;
      case "constant":
        description = "Constant";
        const val = textDecoder.decode(nodeInfo.facts["/kythe/text"]);
        if (val) {
          description += `: ${val}`;
        }
        break;
      default:
        description = kind.charAt(0).toUpperCase() + kind.slice(1);
        break;
    }
    return description;
  }

  // Creates the contents for a pop-up shown when hovering over a symbol.
  // The popup has 2 sections: definition information (including type, location, and snippet),
  // and documentation string (if it exists).
  // TODO(jdelfino): The styling of this is not the best, but it would need to use HTML instead of
  // Markdown to make it any better.
  async makeHoverContents(ticket: string): Promise<monaco.languages.Hover> {
    return this.fetchDocumentation(ticket).then((rval: search.ExtendedDocumentationReply): monaco.languages.Hover => {
      if (!rval || !rval.nodeInfo || !rval.definition) {
        return { contents: [] };
      }

      let popupContents: monaco.IMarkdownString[] = [];

      // A full description is of the form:
      // <node description> defined at <file path>:<line number>
      // <definition snippet>
      // We make a best-effort to create a partial description if any of the metadata is missing.
      let description = this.nodeInfoToMarkdownDescription(rval.nodeInfo);

      const location =
        this.filenameFromAnchor(rval.definition?.anchor) + ":" + this.lineNumberFromAnchor(rval.definition?.anchor);
      if (location.length > 1) {
        if (description.length > 0) {
          description += " defined at " + location;
        } else {
          description = "Defined at " + location;
        }
      }

      if (rval.definition?.anchor?.snippet) {
        if (description.length > 0) {
          description += "\n\n";
        }
        description += "`" + rval.definition.anchor.snippet + "`";
      }

      if (description.length > 0) {
        description = "**Definition**\n\n" + description;
        popupContents.push({
          value: description,
        });
      }

      if (rval.docstring) {
        popupContents.push({ value: "**Documentation**\n\n" + rval.docstring });
      }

      return { contents: popupContents };
    });
  }

  hoverHandler(
    model: monaco.editor.ITextModel,
    position: monaco.Position
  ): Promise<monaco.languages.Hover> | undefined {
    let ticks = this.ticketsForPosition(position);
    if (!ticks?.length) {
      return;
    }

    this.updateSymbolHighlights(model, ticks);
    return this.makeHoverContents(ticks[0]);
  }

  async fetchDocumentation(tick: string): Promise<search.ExtendedDocumentationReply> {
    const req = new search.KytheRequest({
      docsRequest: new search.ExtendedDocumentationRequest({
        ticket: tick,
      }),
    });
    return rpcService.service
      .kytheProxy(req)
      .then((rsp) => {
        if (!rsp.docsReply) {
          return new search.ExtendedDocumentationReply();
        }
        return rsp.docsReply;
      })
      .catch((e) => {
        console.error("Error fetching documentation for ticket", tick, e);
        return new search.ExtendedDocumentationReply();
      });
  }

  async fetchDecorations(filename: string) {
    if (!filename) {
      return;
    }

    let ticket = "kythe://buildbuddy?path=" + filename;
    const req = new search.KytheRequest({
      decorationsRequest: new kythe.proto.DecorationsRequest({
        location: new kythe.proto.Location({
          ticket: ticket,
        }),
        references: true,
        targetDefinitions: true,
        semanticScopes: true,
        diagnostics: true,
      }),
    });
    let rsp = await rpcService.service.kytheProxy(req);

    const newDecor =
      rsp.decorationsReply?.reference
        .map((x) => {
          const startLine = x.span?.start?.lineNumber || 0;
          const startColumn = x.span?.start?.columnOffset || 0;
          const endLine = x.span?.end?.lineNumber || 0;
          const endColumn = x.span?.end?.columnOffset || 0;
          const monacoRange = new monaco.Range(startLine, startColumn + 1, endLine, endColumn + 1);
          const displayOptions = this.getDisplayOptions(x);
          if (displayOptions === null) {
            return null;
          }
          // slight hack: store the kythe reference in the "after" injected text property - it has
          // a field that holds a generic object. This allows the kythe references to be easily
          // retrieved later.
          displayOptions.after = { attachedData: x, content: "" };

          return {
            range: monacoRange,
            options: displayOptions,
          };
        })
        .filter((x) => x !== null) || [];
    return newDecor;
  }

  getChange(path: string) {
    return this.state.changes.get(path);
  }

  fetchIfNeededAndNavigate(path: string, additionalParams = "", line = 0): Promise<boolean> {
    if (line > 0) {
      this.navigateToPathWithLine(path, line);
    } else {
      this.navigateToPath(path + additionalParams);
    }

    if (this.state.mergeConflicts.has(path)) {
      this.handleViewConflictClicked(path, this.state.mergeConflicts.get(path)!, undefined);
      return Promise.resolve(true);
    } else if (this.isDiffView()) {
      this.handleViewDiffClicked(path);
      return Promise.resolve(true);
    }

    return this.getModel(path).then((model) => {
      this.setModel(path, model);
      return true;
    });
  }

  async getModel(path: string) {
    if (this.state.fullPathToModelMap.has(path)) {
      return this.state.fullPathToModelMap.get(path);
    }

    return await this.fetchContentForPath(path).then((response) => {
      if (!response.file) {
        throw `File not found for path ${path}`;
      }
      return this.ensureModelExists(path, response.file);
    });
  }

  parseGithubUrl(githubUrl: string) {
    let match = githubUrl.match(
      /^(https:\/\/github.com)?(\/)?(?<owner>[A-Za-z0-9_.-]+)?(\/(?<repo>[A-Za-z0-9_.-]+))?(\/(?<entity>[A-Za-z0-9_.-]+))?(\/(?<rest>.*))?/
    )?.groups;

    let destinationUrl = "/code/";
    if (match?.owner) {
      destinationUrl += match?.owner + "/";
    }
    if (match?.repo) {
      destinationUrl += match?.repo + "/";
    }
    if (match?.entity == "tree" && match?.rest) {
      let parts = match?.rest.split("/");
      let branch = parts.shift();
      destinationUrl += "?branch=" + branch;
    } else if (match?.entity == "blob" && match?.rest) {
      let parts = match?.rest.split("/");
      let branch = parts.shift();
      destinationUrl += parts.join("/") + "?branch=" + branch;
    } else if (match?.entity == "pull") {
      destinationUrl = `/reviews/${match?.owner}/${match?.repo}/${match?.rest.split("/")[0]}`;
    }
    return destinationUrl;
  }

  fetchContentForPath(path: string, sha?: string) {
    return rpcService.service.getWorkspaceFile(
      new workspace.GetWorkspaceFileRequest({
        workspace: this.currentWorkspaceName(),
        repo: this.getRepo(),
        file: new workspace.Node({
          path: path,
          sha: sha,
        }),
      })
    );
  }

  getRepo() {
    return new workspace.Repo({
      repoUrl: `https://github.com/${this.currentOwner()}/${this.currentRepo()}.git`,
      commitSha: this.getCommit(),
      branch: this.getBranch(),
    });
  }

  getRef() {
    return this.getCommit() || this.getBranch();
  }

  getCommit() {
    return this.props.search.get("commit") || this.state.directoryResponse?.directory?.sha || "";
  }

  getDefaultBranch() {
    return this.state.repoResponse?.defaultBranch;
  }

  getBranch() {
    return this.props.search.get("branch") || this.getDefaultBranch();
  }

  getQuery() {
    return this.props.search.get("pq");
  }

  decorToReference(decor: monaco.editor.IModelDecoration): kythe.proto.DecorationsReply.Reference | undefined {
    return decor?.options?.after?.attachedData as kythe.proto.DecorationsReply.Reference | undefined;
  }

  getKytheRefsForRange(range: monaco.Range): kythe.proto.DecorationsReply.Reference[] {
    // This finds the smallest decorations in a given range.
    // All decorations that share the same smallest span will be returned.
    // This is necessary because, for example, some definitions will have multiple
    // /kythe/edge/defines/binding edges, and we want to process them all so we don't
    // miss references.
    const decorInRange = this.editor?.getDecorationsInRange(range);
    if (!decorInRange) {
      return [];
    }

    let refsInRange = decorInRange.map((decor) => this.decorToReference(decor)).filter((ref) => !!ref);

    let minMatches: kythe.proto.DecorationsReply.Reference[] = [];
    let minMatchLength = Number.POSITIVE_INFINITY;
    for (const ref of refsInRange) {
      if (!(ref?.span?.start && ref?.span?.end)) {
        continue;
      }
      const matchLength = ref.span.end.byteOffset - ref.span.start.byteOffset;
      if (matchLength < minMatchLength) {
        minMatchLength = matchLength;
        minMatches = [ref];
      } else if (
        // If this span has the same offsets as the current minimum, return it also.
        matchLength === minMatchLength &&
        minMatches[0]?.span?.start?.byteOffset === ref.span.start.byteOffset &&
        minMatches[0]?.span?.end?.byteOffset === ref.span.end.byteOffset
      ) {
        minMatches.push(ref);
      }
    }

    return minMatches;
  }

  fetchInitialContent() {
    if (this.fetchedInitialContent || !this.getDefaultBranch()) {
      return;
    }
    this.fetchedInitialContent = true;

    if (!this.currentRepo() && !this.isSingleFile()) {
      return;
    }
    window.addEventListener("resize", () => this.handleWindowResize());
    window.addEventListener("hashchange", () => this.focusLineNumberAndHighlightQuery());
    window.addEventListener("mouseup", () => {
      window.removeEventListener("mousemove", this.resizeXrefsProp, false);
    });

    this.editor = monaco.editor.create(this.codeViewer.current!, {
      value: "",
      theme: "vs",
      readOnly: this.isSingleFile() || Boolean(this.getQuery()),
    });
    this.searchDecorations = this.editor.createDecorationsCollection();
    this.lcovDecorations = this.editor.createDecorationsCollection();

    this.editor.onMouseDown((e) => {
      this.mousedownTarget = e.target.position ?? undefined;
    });

    this.editor.onMouseUp((e) => {
      if (this.mousedownTarget && e.target.position) {
        if (
          e.target.position.column === this.mousedownTarget.column &&
          e.target.position.lineNumber === this.mousedownTarget.lineNumber
        ) {
          this.navigateToDefinitionOrPopulatePanel(e.target.position);
        }
      } else {
        this.mousedownTarget = undefined;
      }
    });

    this.goToDefKey = this.editor.createContextKey<boolean>("goToDefContextKey", false);
    this.editor.addAction({
      id: "code-search-definition-action",
      label: "Go to definition",
      precondition: "goToDefContextKey",
      contextMenuGroupId: "navigation",
      contextMenuOrder: 1,
      // Method that will be executed when the action is triggered.
      run: (ed) => {
        const pos = ed.getPosition();
        if (!pos) {
          return;
        }
        this.navigateToDefinition(this.ticketsForPosition(pos));
      },
    });

    this.findRefsKey = this.editor.createContextKey<boolean>("findRefsContextKey", false);
    this.editor.addAction({
      id: "code-search-reference-action",
      label: "Find references",
      precondition: "findRefsContextKey",
      contextMenuGroupId: "navigation",
      contextMenuOrder: 2,
      // Method that will be executed when the action is triggered.
      run: (ed) => {
        const pos = ed.getPosition();
        if (!pos) {
          return;
        }
        this.populateXrefsPanel(this.ticketsForPosition(pos));
      },
    });

    this.editor.onContextMenu((e) => {
      if (e.target.range) {
        let decors = this.editor?.getDecorationsInRange(e.target.range);
        // TODO(jdelfino): Disable "Go to definition" when already on a definition
        this.findRefsKey?.set(decors != null && decors.length > 0);
        this.goToDefKey?.set(decors != null && decors.length > 0);
      }
    });

    monaco.languages.registerHoverProvider(
      { scheme: "file" },
      {
        provideHover: this.hoverHandler.bind(this),
      }
    );

    this.forceUpdate();

    const bytestreamURL = this.props.search.get("bytestream_url") || "";
    const compareBytestreamURL = this.props.search.get("compare_bytestream_url") || "";
    if (compareBytestreamURL) {
      this.showBytestreamCompare();
      return;
    } else if (this.isSingleFile() && bytestreamURL) {
      this.showBytestreamFile();
      return;
    }
    if (this.isSingleFile() && this.isLcov()) {
      this.showLcov();
      return;
    }

    if (this.currentPath()) {
      const url = new URL(window.location.href);
      this.fetchIfNeededAndNavigate(this.currentPath(), "?" + url.searchParams.toString());
    } else {
      this.editor.setValue(
        ["// Welcome to BuildBuddy Code!", "", "// Click on a file to the left to get start editing."].join("\n")
      );
    }

    this.editor.onDidChangeModelContent(() => {
      this.handleContentChanged();
      this.highlightQuery();
    });
  }

  highlightQuery() {
    if (this.getQuery()) {
      let ranges = this.editor
        ?.getModel()
        ?.findMatches(
          this.getQuery()!,
          /* searchOnlyEditableRanges= */ false,
          /* isRegex= */ true,
          /* matchCase= */ false,
          /* wordSeparators= */ null,
          /* captureMatches= */ false
        );
      if (!ranges || ranges.length == 0) {
        this.searchDecorations?.clear();
        return;
      }

      this.searchDecorations?.set(
        ranges?.map((r) => {
          return { range: r.range, options: { inlineClassName: "code-query-highlight" } };
        })
      );
    }
  }

  focusLineNumberAndHighlightQuery() {
    let focusedLineNumber = window.location.hash.startsWith("#L") ? parseInt(window.location.hash.substr(2)) : 0;
    setTimeout(() => {
      this.editor?.setSelection(new monaco.Selection(focusedLineNumber, 0, focusedLineNumber, 0));
      this.editor?.revealLinesInCenter(focusedLineNumber, focusedLineNumber);
      this.editor?.focus();
      this.highlightQuery();
    });
  }

  async getOriginalFileContents(path: string) {
    if (this.state.originalFileContents.has(path)) {
      return this.state.originalFileContents.get(path);
    }

    let node = this.getChange(path);
    if (!node) {
      node = new workspace.Node({
        path: path,
      });
    }

    return rpcService.service
      .getWorkspaceFile(
        new workspace.GetWorkspaceFileRequest({
          workspace: this.currentWorkspaceName(),
          repo: this.getRepo(),
          file: new workspace.Node({
            originalSha: node.originalSha,
            path: node.path,
            nodeType: node.nodeType,
            sha: node.originalSha,
          }),
        })
      )
      .then((response) => {
        if (!response.file) {
          throw `File not found for path ${this.currentPath()}`;
        }
        let contents = textDecoder.decode(response.file.content);
        this.state.originalFileContents.set(path, contents);
        return contents;
      });
  }

  async handleContentChanged() {
    if ((await this.getOriginalFileContents(this.currentPath())) === this.editor?.getValue()) {
      this.state.changes.delete(this.currentPath());
      this.state.fullPathToIncludeChanges.delete(this.currentPath());
    } else if (this.currentPath()) {
      if (!this.state.changes.get(this.currentPath())) {
        this.state.fullPathToIncludeChanges.set(this.currentPath(), true);
      }

      if (this.state.changes.has(this.currentPath())) {
        let node = this.state.changes.get(this.currentPath());
        if (!node) {
          throw `Node not found ${this.currentPath()}`;
        }

        this.state.changes.set(
          this.currentPath(),
          new workspace.Node({
            changeType: node.changeType,
            content: textEncoder.encode(this.editor?.getValue()),
            originalSha: node.originalSha,
            path: node.path,
            nodeType: node.nodeType,
            sha: node.sha,
          })
        );
        console.log(node);
      } else {
        let node = this.state.fullPathToNodeMap.get(this.currentPath());
        if (!node) {
          throw `Node not found ${this.currentPath()}`;
        }
        node.changeType = workspace.ChangeType.MODIFIED;
        node.content = textEncoder.encode(this.editor?.getValue() || "");
        this.state.changes.set(this.currentPath(), node);
      }
    }
    this.updateState({ changes: this.state.changes });
  }

  private timeout?: any;

  saveState() {
    window.clearTimeout(this.timeout);
    this.timeout = setTimeout(async () => {
      let changes = await Promise.all(
        Array.from([...this.state.changes.values()]).map(async (change) => {
          console.log("change", change);
          if (change.content.length) {
            let sha = await sha1(change.content);
            console.log(`${sha} == ${change.sha}`);
            if (sha == change.sha) {
              change.content = new Uint8Array();
            }
            this.state.changes.get(change.path)!.sha = sha;
          }
          return change;
        })
      );

      console.log(changes);

      rpcService.service.saveWorkspace(
        new workspace.SaveWorkspaceRequest({
          workspace: new workspace.Workspace({
            name: this.currentWorkspaceName(),
            repo: this.getRepo(),
            changes: changes,
          }),
        })
      );
    }, 1000);
    // TODO: show a little saved indicator
  }

  componentWillUnmount() {
    window.removeEventListener("resize", () => this.handleWindowResize());
    document.removeEventListener("keydown", this.onKeydown);
    this.subscription?.unsubscribe();
    this.editor?.dispose();
  }

  componentDidUpdate(prevProps: Props, prevState: State) {
    this.fetchInitialContent();

    const path = this.currentPath();
    this.getModel(path).then((model) => {
      this.setModel(path, model);
      return true;
    });
    this.focusLineNumberAndHighlightQuery();
  }

  parsePath() {
    let groups = this.props.path?.match(/\/code\/(?<owner>[^\/]*)\/(?<repo>[^\/]*)(\/(?<path>.*))?/)?.groups;
    return groups || {};
  }

  currentOwner() {
    return this.parsePath().owner;
  }

  currentRepo() {
    return this.parsePath().repo;
  }

  currentPath() {
    return this.parsePath().path || "";
  }

  currentWorkspaceName() {
    return this.currentOwner + "/" + this.currentRepo();
  }

  async fetchCode() {
    await rpcService.service
      .getWorkspace(
        new workspace.GetWorkspaceRequest({
          name: this.currentWorkspaceName(),
        })
      )
      .then((ws) => {
        this.setState({
          changes: new Map(
            ws.workspace!.changes.map((c: workspace.Node) => {
              return [c.path, c];
            })
          ),
        });
        console.log(ws);
      })
      .catch((error) => {
        console.log("no existing workspace found: ", error);
      });

    if (!this.currentOwner() || !this.currentRepo()) {
      return;
    }

    let repoResponse = await rpcService.service.getGithubRepo(
      new github.GetGithubRepoRequest({ owner: this.currentOwner(), repo: this.currentRepo() })
    );
    console.log(repoResponse);

    let repo = this.getRepo();
    repo.branch = repo.branch || repoResponse.defaultBranch;

    rpcService.service
      .getWorkspaceDirectory(
        new workspace.GetWorkspaceDirectoryRequest({
          workspace: this.currentWorkspaceName(),
          repo: repo,
          directory: new workspace.Node({
            path: "",
          }),
        })
      )
      .then((response) => {
        console.log(response);
        this.updateState({
          repoResponse: repoResponse,
          directoryResponse: response,
        });
      });
  }

  isNewFile(node: workspace.Node) {
    return !node.sha;
  }

  isDirectory(node: workspace.Node) {
    return node.nodeType === workspace.NodeType.DIRECTORY;
  }

  async modelForBytestreamUrl(bytestreamURL: string, invocationID: string, path: string, zip?: string) {
    let model = getOrCreateModel(path + "-error", "File not found in cache.");
    await rpcService
      .fetchBytestreamFile(bytestreamURL, invocationID, "text", { zip })
      .then((result) => {
        model = getOrCreateModel(path, result);
      })
      .catch(() => {});
    return model;
  }

  async showBytestreamCompare() {
    this.setState({ loading: true });
    const bytestreamURL = this.props.search.get("bytestream_url") || "";
    const invocationID = this.props.search.get("invocation_id") || "";
    const zip = this.props.search.get("z") || undefined;
    let filename = this.props.search.get("filename") || "file";

    const compareBytestreamURL = this.props.search.get("compare_bytestream_url") || "";
    const compareInvocationID = this.props.search.get("compare_invocation_id") || "";
    let compareFilename = this.props.search.get("compare_filename") || "";

    let modelA = this.modelForBytestreamUrl(bytestreamURL, invocationID, filename, zip);
    let modelB = this.modelForBytestreamUrl(compareBytestreamURL, compareInvocationID, "diff-" + compareFilename, zip);

    if (!this.diffEditor) {
      this.diffEditor = monaco.editor.createDiffEditor(this.diffViewer.current!);
    }
    let diffModel = { original: await modelA, modified: await modelB };
    this.diffEditor?.setModel(diffModel);
    this.state.fullPathToDiffModelMap.set(filename, diffModel);
    this.updateState({ loading: false, fullPathToDiffModelMap: this.state.fullPathToDiffModelMap }, () => {
      this.diffEditor?.layout();
    });
  }

  async showBytestreamFile() {
    this.setState({ loading: true });
    const bytestreamURL = this.props.search.get("bytestream_url") || "";
    const invocationID = this.props.search.get("invocation_id") || "";
    const zip = this.props.search.get("z") || undefined;
    let filename = this.props.search.get("filename") || "file";

    let modelA = await this.modelForBytestreamUrl(bytestreamURL, invocationID, filename, zip);
    this.setState({ loading: false });

    this.setModel(filename, modelA);
  }

  async showLcov() {
    this.setState({ loading: true });
    const lcovURL = this.props.search.get("lcov")!;
    const invocationID = this.props.search.get("invocation_id") || "";

    rpcService.service
      .getWorkspaceFile(
        new workspace.GetWorkspaceFileRequest({
          workspace: this.currentWorkspaceName(),
          repo: this.getRepo(),
          file: new workspace.Node({
            path: this.currentPath(),
          }),
        })
      )
      .then((response) => {
        if (!response.file) {
          throw `File not found for path ${this.currentPath()}`;
        }
        this.navigateToNode(this.currentPath(), response.file);
        rpcService.fetchBytestreamFile(lcovURL, invocationID, "text").then((result) => {
          let records = parseLcov(result);
          for (let record of records) {
            if (record.sourceFile == this.currentPath()) {
              this.lcovDecorations?.set(
                record.data.map((r) => {
                  const parts = r.split(",");
                  const lineNum = parseInt(parts[0]);
                  const hit = parts[1] == "1";
                  return {
                    range: new monaco.Range(lineNum, 0, lineNum, 0),
                    options: {
                      isWholeLine: true,
                      className: hit ? "codeCoverageHit" : "codeCoverageMiss",
                      marginClassName: hit ? "codeCoverageHit" : "codeCoverageMiss",
                      minimap: { color: hit ? "#c5e1a5" : "#ef9a9a", position: 1 },
                    },
                  };
                })
              );
            }
          }
          console.log(result);
        });
      })
      .finally(() => {
        this.setState({ loading: false });
      });
  }

  isPathExpanded(fullPath: string) {
    return this.state.fullPathToExpanded.get(fullPath) || this.currentPath().startsWith(fullPath);
  }

  expandPath(fullPath: string) {
    this.state.fullPathToExpanded.set(fullPath, true);
    this.forceUpdate();
  }

  collapsePath(fullPath: string) {
    this.state.fullPathToExpanded.set(fullPath, false);
    this.forceUpdate();
  }

  isRenaming(fullPath: string) {
    return this.state.fullPathToRenaming.get(fullPath) || false;
  }

  startRenaming(fullPath: string) {
    this.state.fullPathToRenaming.set(fullPath, true);
    this.forceUpdate();
  }

  stopRenaming(fullPath: string) {
    this.state.fullPathToRenaming.set(fullPath, false);
    this.forceUpdate();
  }

  getChildren(fullPath: string, sha: string) {
    if (this.state.fullPathToChildrenMap.has(fullPath)) {
      return this.state.fullPathToChildrenMap.get(fullPath) || [];
    }

    this.state.fullPathToChildrenMap.set(fullPath, []);

    rpcService.service
      .getWorkspaceDirectory(
        new workspace.GetWorkspaceDirectoryRequest({
          workspace: this.currentWorkspaceName(),
          repo: this.getRepo(),
          directory: new workspace.Node({
            path: fullPath,
            sha: sha,
          }),
        })
      )
      .then((response) => {
        this.expandPath(fullPath);
        this.state.fullPathToChildrenMap.set(fullPath, response.childNodes);
        this.updateState({
          fullPathToChildrenMap: this.state.fullPathToChildrenMap,
        });
        console.log(response);
      });

    return [];
  }

  addChild(parentPath: string, newPath: string) {
    let children = this.state.fullPathToChildrenMap.get(parentPath);
    if (parentPath == "" || parentPath == "/") {
      children = this.state.directoryResponse?.childNodes;
    }
    if (!children) {
      children = [];
    }
    children.push(
      new workspace.Node({
        path: newPath.split("/").pop(),
        nodeType: workspace.NodeType.FILE,
        changeType: workspace.ChangeType.ADDED,
      })
    );
    if (this.state.directoryResponse && (parentPath == "" || parentPath == "/")) {
      this.state.directoryResponse.childNodes = children;
    } else {
      this.state.fullPathToChildrenMap.set(parentPath, children);
    }
  }

  // TODO(siggisim): Support moving files around
  handleFileClicked(node: workspace.Node, fullPath: string) {
    if (this.isDirectory(node)) {
      if (this.isPathExpanded(fullPath)) {
        this.collapsePath(fullPath);
        return;
      }

      this.expandPath(fullPath);
      return;
    }

    rpcService.service
      .getWorkspaceFile(
        new workspace.GetWorkspaceFileRequest({
          workspace: this.currentWorkspaceName(),
          repo: this.getRepo(),
          file: new workspace.Node({
            path: fullPath,
            sha: node.sha,
          }),
        })
      )
      .then((response) => {
        console.log(response);
        if (!response.file) {
          throw `File not found for path ${fullPath}`;
        }
        this.navigateToNode(fullPath, response.file);
        this.navigateToPath(fullPath);
      });
  }

  ensureModelExists(fullPath: string, node: workspace.Node) {
    let fileContents = textDecoder.decode(node.content);
    let model = this.state.fullPathToModelMap.get(fullPath);
    if (!model) {
      model = monaco.editor.createModel(fileContents, getLangHintFromFilePath(fullPath), monaco.Uri.file(fullPath));
      this.state.fullPathToModelMap.set(fullPath, model);
      this.state.fullPathToNodeMap.set(fullPath, node);

      this.fetchDecorations(fullPath).then((newDecs) => {
        if (!newDecs) {
          return;
        }
        let oldDecs = this.state.fullPathToDecorationsMap.get(fullPath) || [];
        let newDecIds = model!.deltaDecorations(oldDecs, newDecs);
        this.state.fullPathToDecorationsMap.set(fullPath, newDecIds);
      });
      this.updateState({ fullPathToModelMap: this.state.fullPathToModelMap });
    }
    return model;
  }

  handleTabClicked(fullPath: string) {
    this.fetchIfNeededAndNavigate(fullPath);
  }

  navigateToNode(fullPath: string, node: workspace.Node) {
    this.setModel(fullPath, this.ensureModelExists(fullPath, node));
  }

  setModel(fullPath: string, model: monaco.editor.ITextModel | undefined) {
    if (!this.state.tabs.has(fullPath)) {
      this.state.tabs.set(fullPath, fullPath);
      this.updateState({ tabs: this.state.tabs }, () => this.focusLineNumberAndHighlightQuery());
    }
    this.editor?.setModel(model || null);
  }

  navigateToPath(path: string) {
    window.history.pushState(
      undefined,
      "",
      `/code/${this.currentOwner()}/${this.currentRepo()}/${path}${window.location.hash}`
    );
  }

  navigateToPathWithLine(path: string, line: number) {
    window.history.pushState(undefined, "", `/code/${this.currentOwner()}/${this.currentRepo()}/${path}#L${line}`);
  }

  async fetchKytheData(req: search.KytheRequest): Promise<search.KytheResponse> {
    this.pendingXrefsRequest?.cancel();
    this.setState({ xrefsLoading: true });

    let xrefReq = rpcService.service.kytheProxy(req);
    this.pendingXrefsRequest = xrefReq;
    return xrefReq.finally(() => {
      this.setState({ xrefsLoading: false });
    });
  }

  async handleBuildClicked(args: string) {
    let newCommands = this.state.commands;
    // Remove if it already exists.
    const index = newCommands.indexOf(args);
    if (index > -1) {
      newCommands.splice(index, 1);
    }
    // Place it at the front.
    newCommands.unshift(args);
    // Limit the number of commands.
    newCommands = newCommands.slice(0, 10);

    let request = new runner.RunRequest();
    request.gitRepo = new git.GitRepo();
    request.gitRepo.repoUrl = `https://github.com/${this.currentOwner()}/${this.currentRepo()}.git`;
    request.bazelCommand = args + (this.state.defaultConfig ? ` --config=${this.state.defaultConfig}` : "");
    request.repoState = await this.getRepoState();
    request.async = true;
    request.runRemotely = true;
    request.execProperties = [
      new build.bazel.remote.execution.v2.Platform.Property({ name: "include-secrets", value: "true" }),
    ];

    this.updateState({ isBuilding: true, commands: newCommands });
    rpcService.service
      .run(request)
      .then((response: runner.RunResponse) => {
        window.open(`/invocation/${response.invocationId}?queued=true`, "_blank");
      })
      .catch((error) => {
        alert(error);
      })
      .finally(() => {
        this.updateState({ isBuilding: false });
      });
  }

  async getRepoState() {
    let state = new git.RepoState();
    state.commitSha = this.getCommit();
    state.branch = this.getBranch()!;
    for (let path of this.state.changes.keys()) {
      state.patch.push(
        textEncoder.encode(
          diff.createTwoFilesPatch(
            `a/${path}`,
            `b/${path}`,
            (await this.getOriginalFileContents(path)) || "",
            textDecoder.decode(this.state.changes.get(path)?.content) || ""
          )
        )
      );
    }
    return state;
  }

  async handleShowReviewModalClicked() {
    await new Promise((resolve) => this.handleUpdateCommitSha(resolve));

    let installationResponse = await rpcService.service.getGithubUserInstallations(
      new github.GetGithubUserInstallationsRequest()
    );

    if (this.state.mergeConflicts.size > 0) {
      alert_service.error(
        `You must resolve the ${this.state.mergeConflicts.size} merge conflicts before requesting a review.`
      );
      return;
    }

    let filteredEntries = Array.from(this.state.changes.entries()).filter(([key, value]) =>
      this.state.fullPathToIncludeChanges.get(key)
    );
    let filenames = filteredEntries.map(([key, value]) => key).join(", ");
    this.updateState({
      prTitle: `Update ${filenames}`,
      prBody: `Update ${filenames}`,
      reviewRequestModalVisible: true,
      installationsResponse: installationResponse,
    });
  }

  handleReviewClicked() {
    if (!this.props.user.githubLinked) {
      this.handleGitHubClicked();
      return;
    }

    this.updateState({ requestingReview: true });

    let filteredEntries = Array.from(this.state.changes.entries()).filter(
      ([key, value]) => this.state.fullPathToIncludeChanges.get(key) // Only include checked changes
    );

    createPullRequest({
      owner: this.currentOwner(),
      repo: this.currentRepo(),
      title: this.state.prTitle,
      body: this.state.prBody,
      head: `change-${Math.floor(Math.random() * 10000)}`,
      changes: [
        {
          files: Object.fromEntries(
            filteredEntries.map(([key, value]) => (value ? [key, { content: value.content }] : [key, null]))
          ),
          commit: this.state.prBody,
        },
      ],
    })
      .then((response) => {
        let prLink = capabilities.config.codeReviewEnabled
          ? router.getReviewUrl(this.currentOwner(), this.currentRepo(), +response.pullNumber)
          : response.url;
        this.updateState({
          requestingReview: false,
          reviewRequestModalVisible: false,
          prLink: prLink,
          prNumber: response.pullNumber,
          prBranch: response.ref,
        });
        window.open(prLink, "_blank");
        console.log(response);
      })
      .catch((e) => {
        error_service.handleError(e);
        this.updateState({
          requestingReview: false,
        });
      });
  }

  handleChangeClicked(fullPath: string) {
    if (this.state.changes.get(fullPath)?.changeType == workspace.ChangeType.DELETED) {
      return;
    }
    this.fetchIfNeededAndNavigate(fullPath);
  }

  handleCheckboxClicked(fullPath: string) {
    this.state.fullPathToIncludeChanges.set(fullPath, !this.state.fullPathToIncludeChanges.get(fullPath));
    this.updateState({ fullPathToIncludeChanges: this.state.fullPathToIncludeChanges });
  }

  handleDeleteClicked(fullPath: string, node: workspace.Node) {
    if (this.isDirectory(node)) {
      error_service.handleError("Deleting directories is not yet supported");
      return;
    }
    if (this.isNewFile(node)) {
      this.state.changes.delete(fullPath);
      this.state.fullPathToIncludeChanges.delete(fullPath);
    } else {
      this.state.changes.set(
        fullPath,
        new workspace.Node({ path: fullPath, changeType: workspace.ChangeType.DELETED, sha: node.sha })
      );
      this.state.fullPathToIncludeChanges.set(fullPath, true);
    }

    this.updateState({ changes: this.state.changes });
  }

  newFileWithContents(parentNode: workspace.Node, path: string, contents: string) {
    let parent = this.getParent(path);
    let tempFiles = this.state.temporaryFiles.get(parent);
    if (tempFiles) {
      tempFiles.splice(tempFiles.indexOf(parentNode), 1);
      this.state.temporaryFiles.set(parent, tempFiles);
    }

    let node = new workspace.Node({
      path: path,
      nodeType: workspace.NodeType.FILE,
      changeType: workspace.ChangeType.ADDED,
    });
    this.state.changes.set(path, node);

    this.addChild(parent, path);

    let model = this.state.fullPathToModelMap.get(path);
    if (!model) {
      model = monaco.editor.createModel(contents, getLangHintFromFilePath(path), monaco.Uri.file(path));
      this.state.fullPathToModelMap.set(path, model);
      this.state.fullPathToNodeMap.set(path, node);
    }
    this.navigateToPath(path);
    this.updateState({}, () => {
      this.setModel(path, model);
      this.handleContentChanged();
    });
  }

  handleRenameClicked(fullPath: string) {
    this.startRenaming(fullPath);
    this.updateState({});
  }

  handleNewFileClicked(node: workspace.Node, path: string) {
    if (node && node.nodeType != workspace.NodeType.DIRECTORY) {
      path = this.getParent(path);
    }

    if (!this.state.temporaryFiles.has(path)) {
      this.state.temporaryFiles.set(path, []);
    }
    this.state.temporaryFiles
      .get(path)!
      .push(
        new workspace.Node({ path: "", nodeType: workspace.NodeType.FILE, changeType: workspace.ChangeType.ADDED })
      );
    this.updateState({ temporaryFiles: this.state.temporaryFiles });
  }

  handleNewFolderClicked(node: workspace.Node, path: string) {
    if (node && node.nodeType != workspace.NodeType.DIRECTORY) {
      path = this.getParent(path);
    }

    if (!this.state.temporaryFiles.has(path)) {
      this.state.temporaryFiles.set(path, []);
    }
    this.state.temporaryFiles
      .get(path)!
      .push(
        new workspace.Node({ path: "", nodeType: workspace.NodeType.DIRECTORY, changeType: workspace.ChangeType.ADDED })
      );
    this.updateState({ temporaryFiles: this.state.temporaryFiles });
  }

  // TODO: Have a better way to manage which features require write permissions
  // and gate them for users that have installed the read-only app.
  handleGitHubClicked() {
    const userID = this.props.user.displayUser.userId?.id || "";
    window.location.href = linkReadWriteGitHubAppURL(userID, "");
  }

  handleUpdatePR() {
    if (!this.props.user.githubLinked) {
      this.handleGitHubClicked();
      return;
    }

    this.updateState({ updatingPR: true });

    let filteredEntries = Array.from(this.state.changes.entries()).filter(
      ([key, value]) => this.state.fullPathToIncludeChanges.get(key) // Only include checked changes
    );

    let filenames = filteredEntries.map(([key, value]) => key).join(", ");

    updatePullRequest({
      owner: this.currentOwner(),
      repo: this.currentRepo(),
      head: this.state.prBranch,
      changes: [
        {
          files: Object.fromEntries(
            filteredEntries.map(([key, value]) => (value ? [key, { content: value.content }] : [key, null]))
          ),
          commit: `Update ${filenames}`,
        },
      ],
    })
      .then(() => {
        window.open(this.state.prLink, "_blank");
      })
      .catch((e) => {
        error_service.handleError(e);
      })
      .finally(() => {
        this.updateState({ updatingPR: false });
      });
  }

  handleClearPRClicked() {
    this.updateState({
      prNumber: new Long(0),
      prLink: "",
      prBranch: "",
    });
  }

  handleMergePRClicked() {
    rpcService.service
      .mergeGithubPull(
        new github.MergeGithubPullRequest({
          owner: this.currentOwner(),
          repo: this.currentRepo(),
          pullNumber: this.state.prNumber,
        })
      )
      .then(() => {
        window.open(this.state.prLink, "_blank");
        this.handleClearPRClicked();
        this.handleUpdateCommitSha(() => {});
      });
  }

  async handleRevertClicked(node: workspace.Node, event: React.MouseEvent<HTMLSpanElement, MouseEvent>) {
    window.location.hash = "";
    this.state.changes.delete(node.path);
    this.state.fullPathToModelMap.get(node.path)?.setValue((await this.getOriginalFileContents(node.path)) || "");
    this.updateState({ changes: this.state.changes, fullPathToModelMap: this.state.fullPathToModelMap });
    event.stopPropagation();
  }

  // If a callback is set, alert messages will not be shown.
  handleUpdateCommitSha(callback?: (conflicts: number) => void) {
    rpcService.service
      .getGithubCompare(
        new github.GetGithubCompareRequest({
          owner: this.currentOwner(),
          repo: this.currentRepo(),
          base: this.getCommit(),
          head: this.getBranch(),
        })
      )
      .then(async (response) => {
        console.log(response);
        let newCommits = response.aheadBy;
        let newSha = response.commits.pop()?.sha;
        if (Number(newCommits) == 0 || !newSha) {
          if (callback) {
            callback(0);
          } else {
            alert_service.success(`You're already up to date!`);
          }
          return;
        }

        let updatedRepo = this.getRepo();
        updatedRepo.commitSha = newSha;

        let conflictCount = 0;
        for (let file of response.files) {
          if (this.state.changes.has(file.name)) {
            let response = await rpcService.service.getWorkspaceFile(
              new workspace.GetWorkspaceFileRequest({
                workspace: this.currentWorkspaceName(),
                repo: updatedRepo,
                file: new workspace.Node({
                  path: this.currentPath(),
                }),
              })
            );
            let newFileContent = textDecoder.decode(response.file?.content);
            this.state.originalFileContents.set(file.name, newFileContent);
            if (response.file?.content == this.state.changes.get(file.name)?.content) {
              this.state.changes.delete(file.name);
              continue;
            }
            this.state.mergeConflicts.set(file.name, file.sha);
            conflictCount++;
          }
        }

        if (this.state.changes.size == 0 && this.state.prBranch) {
          this.handleClearPRClicked();
        }

        rpcService.service
          .getWorkspaceDirectory(
            new workspace.GetWorkspaceDirectoryRequest({
              workspace: this.currentWorkspaceName(),
              repo: updatedRepo,
              directory: new workspace.Node({
                path: "",
              }),
            })
          )
          .then((response) => {
            console.log(response);
            this.updateState({ directoryResponse: response, mergeConflicts: this.state.mergeConflicts }, () => {
              if (callback) {
                callback(conflictCount);
              } else {
                let message = `You were ${newCommits} commits behind head. You are now up to date!`;
                if (conflictCount > 0) {
                  message += ` There are ${conflictCount} conflicts you'll need to resolve below`;
                  alert_service.error(message);
                } else {
                  alert_service.success(message);
                }
              }
              if (this.state.mergeConflicts.has(this.currentPath())) {
                this.handleViewConflictClicked(
                  this.currentPath(),
                  this.state.mergeConflicts.get(this.currentPath())!,
                  undefined
                );
              }
            });
          });
      });
  }

  onTitleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const input = e.target;
    this.updateState({ prTitle: input.value });
  }

  onBodyChange(e: React.ChangeEvent<HTMLTextAreaElement>) {
    const input = e.target;
    this.updateState({ prBody: input.value });
  }

  handleResolveClicked(fullPath: string, event: React.MouseEvent<HTMLSpanElement, MouseEvent>) {
    event.stopPropagation();
    this.state.changes.set(
      fullPath,
      new workspace.Node({
        path: fullPath,
        changeType: workspace.ChangeType.MODIFIED,
        content: textEncoder.encode(this.state.fullPathToDiffModelMap.get(fullPath)?.modified.getValue() || ""),
      })
    );
    this.state.fullPathToDiffModelMap.delete(fullPath);
    this.state.mergeConflicts.delete(fullPath);
    this.updateState({
      changes: this.state.changes,
      fullPathToDiffModelMap: this.state.fullPathToDiffModelMap,
      mergeConflicts: this.state.mergeConflicts,
    });
  }

  async handleViewConflictClicked(
    fullPath: string,
    sha: string,
    event?: React.MouseEvent<HTMLSpanElement, MouseEvent>
  ) {
    event?.stopPropagation();
    return rpcService.service
      .getWorkspaceFile(
        new workspace.GetWorkspaceFileRequest({
          workspace: this.currentWorkspaceName(),
          repo: this.getRepo(),
          file: new workspace.Node({
            path: fullPath,
            sha: sha,
          }),
        })
      )
      .then(async (response) => {
        console.log(response);
        if (!this.diffEditor) {
          this.diffEditor = monaco.editor.createDiffEditor(this.diffViewer.current!);
        }
        let fileContents = textDecoder.decode(response.file?.content);
        let editedModel = await this.getModel(fullPath);
        if (!editedModel) {
          throw `Couldn't get model for ${fullPath}`;
        }
        let uri = monaco.Uri.file(`${sha}-${fullPath}`);
        let latestModel = getOrCreateModel(uri.path, fileContents);
        let diffModel = { original: latestModel, modified: editedModel };
        this.diffEditor.setModel(diffModel);
        this.state.fullPathToDiffModelMap.set(fullPath, diffModel);

        this.navigateToPath(fullPath);
        this.updateState({ fullPathToDiffModelMap: this.state.fullPathToDiffModelMap }, () => {
          this.diffEditor?.layout();
        });
      });
  }

  async handleViewDiffClicked(path: string, event?: React.MouseEvent<HTMLSpanElement, MouseEvent>) {
    let node = this.getChange(path);
    if (!node) {
      throw `Change not found ${path}`;
    }

    event?.stopPropagation();
    if (!this.diffEditor) {
      this.diffEditor = monaco.editor.createDiffEditor(this.diffViewer.current!);
    }

    let editedModel = await this.getModel(node.path);
    if (!editedModel) {
      throw `Couldn't get model for ${node.path}`;
    }

    let uri = monaco.Uri.file(`original-${node.path}`);
    let latestModel = getOrCreateModel(uri.path, (await this.getOriginalFileContents(path)) || "");
    let diffModel = { original: latestModel, modified: editedModel };
    this.diffEditor.setModel(diffModel);
    this.navigateToPath(node.path + "#diff");
    this.updateState({ fullPathToDiffModelMap: this.state.fullPathToDiffModelMap }, () => {
      this.diffEditor?.layout();
    });
  }

  handleCloseReviewModal() {
    this.updateState({ reviewRequestModalVisible: false });
  }

  getParent(path: string) {
    let lastSlashIndex = path.lastIndexOf("/");
    if (lastSlashIndex == -1) {
      lastSlashIndex = 0;
    }
    return path.substr(0, lastSlashIndex);
  }

  joinPath(paths: string[]) {
    return paths.filter((p) => p).join("/");
  }

  isDiffView() {
    return window.location.hash == "#diff";
  }

  async handleRename(node: workspace.Node, path: string, newValue: string, existingFile: boolean) {
    if (!newValue) {
      return;
    }
    this.stopRenaming(path);

    if (this.isDirectory(node)) {
      if (existingFile) {
        error_service.handleError("Renaming directories not yet supported!");
      } else {
        node.path = newValue;
      }
      return;
    }

    if (existingFile) {
      let parent = this.getParent(path);
      let newPath = this.joinPath([parent, newValue]);
      if (newPath == path) {
        return;
      }

      let model = await this.getModel(path);
      if (!model) {
        throw `Couldn't find model for ${path}`;
      }
      this.newFileWithContents(node, newPath, model.getValue());
      this.handleDeleteClicked(path, node);
      this.updateState({});
      return;
    }

    this.newFileWithContents(node, path + newValue, "// Your code here");
  }

  clearContextMenu() {
    this.updateState({
      showContextMenu: false,
      contextMenuX: undefined,
      contextMenuY: undefined,
      contextMenuFile: undefined,
      contextMenuFullPath: undefined,
    });
  }

  handleContextMenu(node: workspace.Node | undefined, fullPath: string, event: React.MouseEvent) {
    if (node && this.isDirectory(node)) {
      this.expandPath(fullPath);
    }
    this.updateState({
      showContextMenu: true,
      contextMenuX: event.pageX,
      contextMenuY: event.pageY,
      contextMenuFile: node,
      contextMenuFullPath: fullPath,
    });
    event.preventDefault();
    event.stopPropagation();
  }

  handleEditClicked() {
    const url = new URL(window.location.href);
    url.searchParams.delete("pq");
    window.location.href = url.href;
  }

  updateState(newState: Partial<State>, callback?: VoidFunction) {
    this.setState(newState as State, () => {
      this.saveState();
      if (callback) {
        callback();
      }
    });
  }

  getFiles(nodes: workspace.Node[], parent: string) {
    // Add any temporary files
    let files = new Map<string, workspace.Node>();
    for (let t of this.state.temporaryFiles.get(parent) || []) {
      files.set(t.path, t);
    }

    // And the original files from github
    for (let o of nodes) {
      files.set(o.path, o);
    }

    return Array.from(files.values());
  }

  filenameFromAnchor(a: kythe.proto.Anchor | null | undefined): string {
    return a?.parent.split("?path=")[1] || "";
  }

  lineNumberFromAnchor(a: kythe.proto.Anchor | null | undefined): number {
    return a?.span?.start?.lineNumber || 0;
  }

  navigateToAnchor(a: kythe.proto.Anchor) {
    this.fetchIfNeededAndNavigate(this.filenameFromAnchor(a), "", this.lineNumberFromAnchor(a));
  }

  renderAnchors(name: string, anchors: kythe.proto.CrossReferencesReply.RelatedAnchor[]) {
    if (anchors.length === 0) {
      return <></>;
    }
    // group up by parent...
    const fileToRefsMap: Map<string, kythe.proto.CrossReferencesReply.RelatedAnchor[]> = new Map();

    anchors.forEach((a) => {
      if (!a.anchor) {
        return;
      }
      const parentTicket = a.anchor.parent;
      if (!fileToRefsMap.has(parentTicket)) {
        fileToRefsMap.set(parentTicket, []);
      }

      fileToRefsMap.get(parentTicket)!.push(a);
    });

    // Unique all the anchors (duplicates are common).
    // Also, sort them by position in the file.
    for (const [key, refs] of fileToRefsMap.entries()) {
      const uniques = new Map<number, kythe.proto.CrossReferencesReply.RelatedAnchor>();
      const uniqueLineRefs = refs.filter((ra) => {
        if (!ra.anchor || !ra.anchor.span || !ra.anchor.span.start) {
          return false;
        }
        if (uniques.has(ra.anchor.span.start.lineNumber)) {
          return false;
        }
        uniques.set(ra.anchor.span.start.lineNumber, ra);
        return true;
      });
      uniqueLineRefs.sort((a, b) => {
        return (
          (a.anchor?.span?.start?.byteOffset || Number.POSITIVE_INFINITY) -
          (b.anchor?.span?.start?.byteOffset || Number.POSITIVE_INFINITY)
        );
      });

      fileToRefsMap.set(key, uniqueLineRefs);
    }

    // Now sort the files, putting non-tests before tests.
    // TODO(jdelfino): Sort suspected genfiles last (e.g. files that match `.pb.*$`)
    let sortedFiles = new Map(
      [...fileToRefsMap.entries()].sort((a, b) => {
        const aTest = a[0].toLowerCase().includes("test");
        const bTest = b[0].toLowerCase().includes("test");
        if (aTest && !bTest) {
          return 1; // a is a test, b is not, so b should come first
        } else if (!aTest && bTest) {
          return -1; // b is a test, a is not, so a should come first
        }
        return a[0] < b[0] ? -1 : 1; // otherwise sort alphabetically
      })
    );

    return (
      <div>
        <div className="xrefs-category">{name}</div>
        {[...sortedFiles.entries()].map(([ticket, anchors]) => {
          const path = new URL(ticket).searchParams.get("path") ?? "";
          if (!path) {
            return <></>;
          }
          return (
            <div>
              <div
                className="xrefs-file"
                onClick={() => {
                  this.fetchIfNeededAndNavigate(path, "", 1);
                }}>
                {path} ({anchors.length} result{anchors.length > 1 ? "s" : ""})
              </div>
              <div className="xrefs-snippet">
                {anchors.map((a) => {
                  return (
                    <div
                      onClick={() => {
                        this.navigateToAnchor(a.anchor!);
                      }}>
                      <span className="xrefs-snippet-line">{a.anchor?.span?.start?.lineNumber}: </span>
                      <span>{a.anchor?.snippet}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  renderXrefPanel() {
    if (!this.state.extendedXrefs) {
      return <></>;
    }

    return (
      <div>
        <div className="xrefs-header">References</div>
        <div className="xrefs-container">
          {Boolean(this.state.extendedXrefs.generatedBy) &&
            this.renderAnchors("Generated By", this.state.extendedXrefs.generatedBy)}
          {Boolean(this.state.extendedXrefs.definitions) &&
            this.renderAnchors("Definitions", this.state.extendedXrefs.definitions)}
          {Boolean(this.state.extendedXrefs.overrides) &&
            this.renderAnchors("Overrides", this.state.extendedXrefs.overrides)}
          {Boolean(this.state.extendedXrefs.overriddenBy) &&
            this.renderAnchors("Overridden By", this.state.extendedXrefs.overriddenBy)}
          {Boolean(this.state.extendedXrefs.extends) && this.renderAnchors("Extends", this.state.extendedXrefs.extends)}
          {Boolean(this.state.extendedXrefs.extendedBy) &&
            this.renderAnchors("Extended By", this.state.extendedXrefs.extendedBy)}
          {Boolean(this.state.extendedXrefs.references) &&
            this.renderAnchors("References", this.state.extendedXrefs.references)}
        </div>
      </div>
    );
  }

  resizeXrefs(e: MouseEvent) {
    this.updateState({
      xrefsHeight: Math.max(100, window.innerHeight - e.clientY - 6),
    });
  }
  resizeXrefsProp = this.resizeXrefs.bind(this);

  render() {
    setTimeout(() => {
      this.editor?.layout();
    }, 0);

    let showDiffView = this.state.fullPathToDiffModelMap.has(this.currentPath()) || this.isDiffView();
    let applicableInstallation = this.state.installationsResponse?.installations.find(
      (i) => i.login == this.currentOwner()
    );
    return (
      <div className="code-editor" onClick={this.clearContextMenu.bind(this)}>
        <div className="code-menu">
          <div className="code-menu-logo">
            {this.isSingleFile() && (
              <a href="javascript:history.back()">
                <ArrowLeft className="code-menu-back" />
              </a>
            )}
            <a href="/">
              <svg
                className="logo"
                width="68"
                height="56"
                viewBox="0 0 68 56"
                fill="none"
                xmlns="http://www.w3.org/2000/svg">
                <path
                  d="M62.8577 29.2897C61.8246 27.7485 60.4825 26.5113 58.8722 25.5604C59.7424 24.8245 60.493 23.998 61.1109 23.0756C62.5071 21.0593 63.1404 18.6248 63.1404 15.8992C63.1404 13.4509 62.7265 11.2489 61.7955 9.37839C60.9327 7.5562 59.6745 6.07124 58.0282 4.96992C56.4328 3.85851 54.5665 3.09733 52.4736 2.64934C50.4289 2.21166 48.1997 2 45.7961 2H4H2V4V52V54H4H46.4691C48.7893 54 51.0473 53.7102 53.2377 53.1272C55.5055 52.5357 57.5444 51.6134 59.3289 50.3425C61.2008 49.0417 62.6877 47.3709 63.7758 45.3524L63.7808 45.3431L63.7857 45.3338C64.9054 43.2032 65.4286 40.7655 65.4286 38.084C65.4286 34.7488 64.6031 31.7823 62.8577 29.2897Z"
                  stroke-width="4"
                  shape-rendering="geometricPrecision"
                />
              </svg>
            </a>
          </div>
          <SearchBar<search.Result>
            placeholder="Search..."
            title="Results"
            fetchResults={async (query) => {
              return (
                await rpcService.service.search(new search.SearchRequest({ query: new search.Query({ term: query }) }))
              ).results;
            }}
            onResultPicked={(result: any, query: string) => {
              this.fetchIfNeededAndNavigate(result.filename, `?pq=${query}&commit=${result.sha}`);
            }}
            emptyState={
              <div className="code-editor-search-bar-empty-state">
                <div className="code-editor-search-bar-empty-state-description">Search for files and code.</div>
                <div className="code-editor-search-bar-empty-state-examples">Examples</div>
                <ul>
                  <li>
                    <code>case:yes Hello World</code>
                  </li>
                  <li>
                    <code>lang:css padding-(left|right)</code>
                  </li>
                  <li>
                    <code>lang:go flag.String</code>
                  </li>
                  <li>
                    <code>filepath:package.json</code>
                  </li>
                </ul>
              </div>
            }
            renderResult={(r) => (
              <div className="code-editor-search-bar-result">
                <div>{r.filename}</div>
                <pre>{r.snippets.map((s) => s.lines).pop()}</pre>
              </div>
            )}
          />

          <OrgPicker user={this.props.user} floating={true} inline={true} />
          {this.isSingleFile() && (
            <div className="code-menu-actions">
              <OutlinedButton
                className="code-menu-download-button"
                onClick={() => {
                  const bsUrl = this.props.search.get("bytestream_url");
                  const invocationId = this.props.search.get("invocation_id");
                  if (!bsUrl || !invocationId) {
                    return;
                  }
                  const zip = this.props.search.get("z");
                  if (zip) {
                    rpcService.downloadBytestreamZipFile(
                      this.props.search.get("filename") || "",
                      bsUrl,
                      zip,
                      invocationId
                    );
                  } else {
                    rpcService.downloadBytestreamFile(this.props.search.get("filename") || "", bsUrl, invocationId);
                  }
                }}>
                <Download /> Download File
              </OutlinedButton>
            </div>
          )}
          {Boolean(this.getQuery()) && (
            <div className="code-menu-actions">
              <OutlinedButton className="request-review-button" onClick={this.handleEditClicked.bind(this)}>
                <Pencil className="icon green" /> Edit
              </OutlinedButton>
            </div>
          )}
          {!this.isSingleFile() && this.currentRepo() && !this.getQuery() && (
            <div className="code-menu-actions">
              {this.state.changes.size > 0 && !this.state.prBranch && (
                <OutlinedButton
                  disabled={this.state.requestingReview}
                  className="request-review-button"
                  onClick={this.handleShowReviewModalClicked.bind(this)}>
                  {this.state.requestingReview ? (
                    <>
                      <Spinner className="icon" /> Requesting...
                    </>
                  ) : (
                    <>
                      <Send className="icon blue" /> Request Review
                    </>
                  )}
                </OutlinedButton>
              )}
              {this.state.changes.size > 0 && this.state.prBranch && (
                <OutlinedButton
                  disabled={this.state.updatingPR}
                  className="request-review-button"
                  onClick={this.handleUpdatePR.bind(this)}>
                  {this.state.updatingPR ? (
                    <>
                      <Spinner className="icon" /> Updating...
                    </>
                  ) : (
                    <>
                      <Send className="icon blue" /> Update PR
                    </>
                  )}
                </OutlinedButton>
              )}
              <CodeBuildButton
                onCommandClicked={this.handleBuildClicked.bind(this)}
                onDefaultConfig={(config) => this.updateState({ defaultConfig: config })}
                isLoading={this.state.isBuilding}
                project={`${this.currentOwner()}/${this.currentRepo()}}`}
                commands={this.state.commands}
                defaultConfig={this.state.defaultConfig}
              />
            </div>
          )}
        </div>
        <div className="code-main">
          {!this.isSingleFile() && (
            <div className="code-sidebar">
              {this.currentOwner() && (
                <div className="code-sidebar-header">
                  <a target="_blank" href={`http://github.com/${this.currentOwner()}`}>
                    {this.currentOwner()}
                  </a>{" "}
                  <ChevronRight />
                  <a target="_blank" href={`http://github.com/${this.currentOwner()}/${this.currentRepo()}`}>
                    {this.currentRepo()}
                  </a>
                </div>
              )}
              <div className="code-sidebar-tree" onContextMenu={(e) => this.handleContextMenu(undefined, "", e)}>
                {this.state.directoryResponse &&
                  this.getFiles(this.state.directoryResponse.childNodes, "")
                    .sort(compareNodes)
                    .map((node) => (
                      <SidebarNodeComponentV2
                        node={node}
                        getFiles={this.getFiles.bind(this)}
                        changes={this.state.changes}
                        isExpanded={this.isPathExpanded.bind(this)}
                        isRenaming={this.isRenaming.bind(this)}
                        getChildren={this.getChildren.bind(this)}
                        handleFileClicked={this.handleFileClicked.bind(this)}
                        fullPath={node.path}
                        handleContextMenu={this.handleContextMenu.bind(this)}
                        handleRename={this.handleRename.bind(this)}
                      />
                    ))}
              </div>
            </div>
          )}
          <div className="code-container">
            {!this.isSingleFile() && !this.getQuery() && (
              <div className="code-viewer-tabs">
                {[...this.state.tabs.keys()].reverse().map((t) => (
                  <div
                    className={`code-viewer-tab ${t == this.currentPath() ? "selected" : ""}`}
                    onClick={this.handleTabClicked.bind(this, t)}>
                    <span>{t.split("/").pop() || "Untitled"}</span>
                    <XCircle
                      onClick={(e) => {
                        this.state.tabs.delete(t);
                        this.updateState({});
                        e.stopPropagation();
                      }}
                    />
                  </div>
                ))}
              </div>
            )}
            <div className="code-viewer-container">
              {!this.currentRepo() && !this.isSingleFile() && <CodeEmptyStateComponent />}
              {this.needsGithubLink() && (
                <div className="code-editor-link-github github-button">
                  <button onClick={this.handleGitHubClicked.bind(this)}>
                    <GithubIcon /> Continue with GitHub
                  </button>
                </div>
              )}
              {this.state.loading && <Spinner className="code-spinner" />}
              <div
                className={`code-viewer ${this.state.loading || this.needsGithubLink() ? "hidden-viewer" : ""} ${
                  showDiffView ? "hidden-viewer" : ""
                }`}
                ref={this.codeViewer}
              />
              <div
                className={`diff-viewer ${this.state.loading || this.needsGithubLink() ? "hidden-viewer" : ""} ${
                  showDiffView ? "" : "hidden-viewer"
                }`}
                ref={this.diffViewer}
              />
            </div>
            {Boolean(this.state.xrefsLoading || this.state.extendedXrefs) && (
              <div>
                <div
                  className="code-search-xrefs-resize"
                  onMouseDown={(e) => {
                    e.preventDefault();
                    window.addEventListener("mousemove", this.resizeXrefsProp, false);
                  }}></div>
                <div className="code-search-xrefs" style={{ height: this.state.xrefsHeight + "px" }}>
                  {/* TODO(jdelfino): Add an error state if xrefs fail to load */}
                  {this.state.xrefsLoading && <div className="loading"></div>}
                  {!this.state.xrefsLoading && this.renderXrefPanel()}
                </div>
              </div>
            )}
            {this.state.changes.size > 0 && !this.getQuery() && (
              <div className="code-diff-viewer">
                <div className="code-diff-viewer-title">
                  Changes{" "}
                  {this.state.prLink && (
                    <span>
                      (
                      <a href={this.state.prLink} target="_blank">
                        PR #{this.state.prNumber}
                      </a>
                      ,{" "}
                      <span className="clickable" onClick={this.handleClearPRClicked.bind(this)}>
                        Clear
                      </span>
                      ,{" "}
                      <span className="clickable" onClick={this.handleMergePRClicked.bind(this)}>
                        Merge
                      </span>
                      )
                    </span>
                  )}
                </div>
                {Array.from(this.state.changes.entries()).map(([fullPath, node]) => (
                  <div className="code-diff-viewer-item" onClick={() => this.handleChangeClicked(fullPath)}>
                    <input
                      checked={this.state.fullPathToIncludeChanges.get(fullPath)}
                      onChange={(event) => this.handleCheckboxClicked(fullPath)}
                      type="checkbox"
                    />{" "}
                    <div
                      className={`code-diff-viewer-item-path${
                        this.state.changes.get(fullPath)?.changeType == workspace.ChangeType.DELETED ? " deleted" : ""
                      }${this.state.changes.get(fullPath)?.changeType == workspace.ChangeType.ADDED ? " added" : ""}`}>
                      {fullPath}
                    </div>
                    {this.state.mergeConflicts.has(fullPath) && fullPath != this.currentPath() && (
                      <span
                        className="code-revert-button"
                        onClick={this.handleViewConflictClicked.bind(
                          this,
                          fullPath,
                          this.state.mergeConflicts.get(fullPath)!
                        )}>
                        View Conflict
                      </span>
                    )}
                    {this.state.mergeConflicts.has(fullPath) && fullPath == this.currentPath() && (
                      <span className="code-revert-button" onClick={this.handleResolveClicked.bind(this, fullPath)}>
                        Resolve Conflict
                      </span>
                    )}
                    {(!this.isDiffView() || fullPath != this.currentPath()) && (
                      <span className="code-revert-button" onClick={this.handleViewDiffClicked.bind(this, fullPath)}>
                        View Diff
                      </span>
                    )}
                    {this.isDiffView() && fullPath == this.currentPath() && (
                      <span className="code-revert-button" onClick={() => (window.location.hash = "")}>
                        Hide Diff
                      </span>
                    )}
                    <span className="code-revert-button" onClick={this.handleRevertClicked.bind(this, node)}>
                      Revert
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
          {!this.getQuery() && (
            <>
              {this.editor &&
                (this.currentPath()?.endsWith("MODULE.bazel") || this.currentPath()?.endsWith("MODULE")) && (
                  <ModuleSidekick editor={this.editor} />
                )}
              {this.editor &&
                (this.currentPath()?.endsWith("BUILD.bazel") || this.currentPath()?.endsWith("BUILD")) && (
                  <BuildFileSidekick editor={this.editor} onBazelCommand={(c) => this.handleBuildClicked(c)} />
                )}
              {this.editor && this.currentPath()?.endsWith(".bazelversion") && (
                <BazelVersionSidekick editor={this.editor} />
              )}
              {this.editor && this.currentPath()?.endsWith(".bazelrc") && <BazelrcSidekick editor={this.editor} />}
            </>
          )}
        </div>
        {this.state.showContextMenu && (
          <div className="context-menu-container">
            <div
              className="context-menu"
              onClick={this.clearContextMenu.bind(this)}
              style={{ top: this.state.contextMenuY, left: this.state.contextMenuX }}>
              <div
                onClick={() => this.handleNewFileClicked(this.state.contextMenuFile!, this.state.contextMenuFullPath!)}>
                New file
              </div>
              <div
                onClick={() =>
                  this.handleNewFolderClicked(this.state.contextMenuFile!, this.state.contextMenuFullPath!)
                }>
                New folder
              </div>
              <div onClick={() => this.handleRenameClicked(this.state.contextMenuFullPath!)}>Rename</div>
              <div
                onClick={() => this.handleDeleteClicked(this.state.contextMenuFullPath!, this.state.contextMenuFile!)}>
                Delete
              </div>
            </div>
          </div>
        )}
        <Modal isOpen={this.state.reviewRequestModalVisible} onRequestClose={this.handleCloseReviewModal.bind(this)}>
          <Dialog className="code-request-review-dialog">
            <DialogHeader>
              <DialogTitle>Request review</DialogTitle>
            </DialogHeader>
            <DialogBody>
              <div className="code-request-review-dialog-body">
                <input value={this.state.prTitle} onChange={this.onTitleChange.bind(this)} />
                <textarea value={this.state.prBody} onChange={this.onBodyChange.bind(this)} />
              </div>
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                {applicableInstallation?.permissions?.pullRequests == "read" && (
                  <FilledButton
                    className="code-request-review-button"
                    onClick={() =>
                      window.open(applicableInstallation?.url + `/permissions/update`, "_blank") &&
                      this.updateState({ installationsResponse: undefined })
                    }>
                    <Key className="icon white" /> Permissions
                  </FilledButton>
                )}
                <FilledButton
                  disabled={this.state.requestingReview || applicableInstallation?.permissions?.pullRequests == "read"}
                  className="code-request-review-button"
                  onClick={this.handleReviewClicked.bind(this)}>
                  {this.state.requestingReview ? (
                    <>
                      <Spinner className="icon white" /> Sending...
                    </>
                  ) : (
                    <>
                      <Send className="icon white" /> Send
                    </>
                  )}
                </FilledButton>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </div>
    );
  }
}

// @ts-ignore
self.MonacoEnvironment = {
  getWorkerUrl: function (workerId: string, label: string) {
    // TODO(siggisim): add language support i.e.
    //https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.25.2/min/vs/basic-languages/go/go.min.js
    return `data:text/javascript;charset=utf-8,${encodeURIComponent(`
      self.MonacoEnvironment = {
        baseUrl: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/${MONACO_VERSION}/min/'
      };
      importScripts('https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/${MONACO_VERSION}/min/vs/base/worker/workerMain.js');`)}`;
  },
};

function getOrCreateModel(url: string, value: string) {
  let existingModel = monaco.editor.getModel(monaco.Uri.file(url));
  if (existingModel) {
    existingModel.setValue(value);
    return existingModel;
  }
  return monaco.editor.createModel(value, getLangHintFromFilePath(url), monaco.Uri.file(url));
}

async function sha1(content: Uint8Array) {
  const hash = await crypto.subtle.digest("SHA-1", content);
  return Array.from(new Uint8Array(hash))
    .map((v) => v.toString(16).padStart(2, "0"))
    .join("");
}
