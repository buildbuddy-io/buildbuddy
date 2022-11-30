import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import SidebarNodeComponent, { compareNodes } from "./code_sidebar_node";
import { Subscription } from "rxjs";
import * as monaco from "monaco-editor";
import { Octokit } from "@octokit/rest";
import * as diff from "diff";
import { runner } from "../../../proto/runner_ts_proto";
import CodeBuildButton from "./code_build_button";
import CodeEmptyStateComponent from "./code_empty";
import { ArrowLeft, ArrowUpCircle, Code, Download, Link, PlusCircle, Send, XCircle } from "lucide-react";
import Spinner from "../../../app/components/spinner/spinner";
import { OutlinedButton, FilledButton } from "../../../app/components/button/button";
import { createPullRequest, updatePullRequest } from "./code_pull_request";
import alert_service from "../../../app/alert/alert_service";

import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Modal from "../../../app/components/modal/modal";

interface Props {
  user: User;
  hash: string;
  path: string;
  search: URLSearchParams;
}
interface State {
  commitSHA: string;
  repoResponse: any;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, any[]>;
  fullPathToModelMap: Map<string, any>;
  fullPathToDiffModelMap: Map<string, any>;
  originalFileContents: Map<string, string>;
  changes: Map<string, string>;
  mergeConflicts: Map<string, string>;
  pathToIncludeChanges: Map<string, boolean>;
  prLink: string;
  prNumber: string;
  prBranch: string;
  prTitle: string;
  prBody: string;

  requestingReview: boolean;
  updatingPR: boolean;
  reviewRequestModalVisible: boolean;
  isBuilding: boolean;
}

const LOCAL_STORAGE_STATE_KEY = "code-state-v1";

// TODO(siggisim): Add links to the code editor from anywhere we reference a repo
// TODO(siggisim): Add branch / workspace selection
// TODO(siggisim): Add some form of search
export default class CodeComponent extends React.Component<Props, State> {
  state: State = {
    commitSHA: "",
    repoResponse: undefined,
    treeShaToExpanded: new Map<string, boolean>(),
    treeShaToChildrenMap: new Map<string, any[]>(),
    fullPathToModelMap: new Map<string, any>(),
    fullPathToDiffModelMap: new Map<string, any>(),
    originalFileContents: new Map<string, any>(),
    changes: new Map<string, string>(),
    mergeConflicts: new Map<string, string>(),
    pathToIncludeChanges: new Map<string, boolean>(),
    prLink: "",
    prBranch: "",
    prNumber: "",

    requestingReview: false,
    updatingPR: false,
    reviewRequestModalVisible: false,
    isBuilding: false,

    prTitle: "",
    prBody: "",
  };

  editor: any;
  diffEditor: any;

  codeViewer = React.createRef<HTMLDivElement>();
  diffViewer = React.createRef<HTMLDivElement>();

  octokit: Octokit;

  subscription: Subscription;

  componentWillMount() {
    document.title = `Code | BuildBuddy`;

    this.updateUser();
    this.fetchCode();

    this.subscription = rpcService.events.subscribe({
      next: (name) => name === "refresh" && this.fetchCode(),
    });
  }

  updateUser() {
    this.octokit = new Octokit({
      auth: this.props.user.githubToken,
    });
  }

  handleWindowResize() {
    this.editor?.layout();
    this.diffEditor?.layout();
  }

  isSingleFile() {
    return Boolean(this.props.search.get("bytestream_url"));
  }

  componentDidMount() {
    if (!this.currentRepo() && !this.isSingleFile()) {
      return;
    }

    window.addEventListener("resize", () => this.handleWindowResize());

    this.editor = monaco.editor.create(this.codeViewer.current, {
      value: ["// Welcome to BuildBuddy Code!", "", "// Click on a file to the left to get start editing."].join("\n"),
      theme: "vs",
      readOnly: this.isSingleFile(),
    });

    let bytestreamURL = this.props.search.get("bytestream_url");
    let invocationID = this.props.search.get("invocation_id");
    let filename = this.props.search.get("filename");
    if (this.isSingleFile()) {
      rpcService.fetchBytestreamFile(bytestreamURL, invocationID, "text").then((result: any) => {
        this.editor.setModel(monaco.editor.createModel(result, undefined, monaco.Uri.file(filename || "file")));
      });
      return;
    }

    if (this.currentPath()) {
      if (this.state.mergeConflicts.has(this.currentPath())) {
        this.handleViewConflictClicked(
          this.currentPath(),
          this.state.mergeConflicts.get(this.currentPath()),
          undefined
        );
      } else {
        this.editor.setModel(this.state.fullPathToModelMap.get(this.currentPath()));
      }
    }

    this.editor.onDidChangeModelContent(() => {
      this.handleContentChanged();
    });
  }

  handleContentChanged() {
    if (this.state.originalFileContents.get(this.currentPath()) === this.editor.getValue()) {
      this.state.changes.delete(this.currentPath());
      this.state.pathToIncludeChanges.delete(this.currentPath());
    } else if (this.currentPath()) {
      if (!this.state.changes.get(this.currentPath())) {
        this.state.pathToIncludeChanges.set(this.currentPath(), true);
      }
      this.state.changes.set(this.currentPath(), this.editor.getValue());
    }
    this.updateState({ changes: this.state.changes });
  }

  saveState() {
    // TODO(siggisim): store this in cache.
    localStorage.setItem(this.getStateCacheKey(), JSON.stringify(this.state, stateReplacer));
  }

  getStateCacheKey() {
    return `${LOCAL_STORAGE_STATE_KEY}/${this.currentOwner()}/${this.currentRepo()}`;
  }

  componentWillUnmount() {
    window.removeEventListener("resize", () => this.handleWindowResize());
    this.subscription?.unsubscribe();
    this.editor?.dispose();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.user != prevProps.user) {
      this.updateUser();
    }
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
    return this.parsePath().path;
  }

  fetchCode() {
    const storedState = localStorage.getItem(this.getStateCacheKey())
      ? (JSON.parse(localStorage.getItem(this.getStateCacheKey()), stateReviver) as State)
      : undefined;
    if (storedState) {
      this.setState(storedState);
      if (storedState.repoResponse && storedState.commitSHA) {
        return;
      }
    }
    if (!this.currentOwner() || !this.currentRepo()) {
      return;
    }

    let commit = this.state.commitSHA || "master";
    this.octokit
      .request(`/repos/${this.currentOwner()}/${this.currentRepo()}/git/trees/${commit}`)
      .then((response: any) => {
        console.log(response);
        this.updateState({ repoResponse: response, commitSHA: response.data.sha });
      });
  }

  // TODO(siggisim): Support deleting files
  // TODO(siggisim): Support moving files around
  // TODO(siggisim): Support renaming files
  // TODO(siggisim): Support right click file context menus
  // TODO(siggisim): Support tabs
  // TODO(siggisim): Remove the use of all `any` types
  handleFileClicked(node: any, fullPath: string) {
    if (node.type === "tree") {
      if (this.state.treeShaToExpanded.get(node.sha)) {
        this.state.treeShaToExpanded.set(node.sha, false);
        this.updateState({ treeShaToExpanded: this.state.treeShaToExpanded });
        return;
      }

      this.octokit
        .request(`/repos/${this.currentOwner()}/${this.currentRepo()}/git/trees/${node.sha}`)
        .then((response: any) => {
          this.state.treeShaToExpanded.set(node.sha, true);
          this.state.treeShaToChildrenMap.set(node.sha, response.data.tree);
          this.updateState({
            treeShaToChildrenMap: this.state.treeShaToChildrenMap,
          });
          console.log(response);
        });
      return;
    }

    this.octokit.rest.git
      .getBlob({
        owner: this.currentOwner(),
        repo: this.currentRepo(),
        file_sha: node.sha,
      })
      .then((response: any) => {
        console.log(response);
        let fileContents = atob(response.data.content);
        this.state.originalFileContents.set(fullPath, fileContents);
        this.updateState({
          originalFileContents: this.state.originalFileContents,
          changes: this.state.changes,
        });
        let model = this.state.fullPathToModelMap.get(fullPath);
        if (!model) {
          model = monaco.editor.createModel(fileContents, undefined, monaco.Uri.file(fullPath));
          this.state.fullPathToModelMap.set(fullPath, model);
          this.updateState({ fullPathToModelMap: this.state.fullPathToModelMap });
        }
        this.editor.setModel(model);
        this.navigateToPath(fullPath);
      });
  }

  navigateToPath(path: string) {
    window.history.pushState(undefined, "", `/code/${this.currentOwner()}/${this.currentRepo()}/${path}`);
  }

  getRemoteEndpoint() {
    // TODO(siggisim): Support on-prem deployments.
    if (window.location.origin.endsWith(".dev")) {
      return "remote.buildbuddy.dev";
    }
    return "remote.buildbuddy.io";
  }

  getJobCount() {
    return 200;
  }

  getContainerImage() {
    return "docker://gcr.io/flame-public/buildbuddy-ci-runner:latest";
  }

  getBazelFlags() {
    return `--remote_executor=${this.getRemoteEndpoint()} --bes_backend=${this.getRemoteEndpoint()} --bes_results_url=${
      window.location.origin
    }/invocation/ --jobs=${this.getJobCount()} --remote_default_exec_properties=container-image=${this.getContainerImage()}`;
  }

  handleBuildClicked(args: string) {
    let request = new runner.RunRequest();
    request.gitRepo = new runner.RunRequest.GitRepo();
    request.gitRepo.repoUrl = `https://github.com/${this.currentOwner()}/${this.currentRepo()}.git`;
    request.bazelCommand = `${args} ${this.getBazelFlags()}`;
    request.repoState = this.getRepoState();

    this.updateState({ isBuilding: true });
    rpcService.service
      .run(request)
      .then((response: runner.RunResponse) => {
        window.open(`/invocation/${response.invocationId}`, "_blank");
      })
      .catch((error: any) => {
        alert(error);
      })
      .finally(() => {
        this.updateState({ isBuilding: false });
      });
  }

  getRepoState() {
    let state = new runner.RunRequest.RepoState();
    state.commitSha = this.state.commitSHA;
    var enc = new TextEncoder();
    for (let path of this.state.changes.keys()) {
      state.patch.push(
        enc.encode(
          diff.createTwoFilesPatch(
            `a/${path}`,
            `b/${path}`,
            this.state.originalFileContents.get(path),
            this.state.changes.get(path)
          )
        )
      );
    }
    return state;
  }

  async handleShowReviewModalClicked() {
    await new Promise((resolve) => this.handleUpdateCommitSha(resolve));

    if (this.state.mergeConflicts.size > 0) {
      alert_service.error(
        `You must resolve the ${this.state.mergeConflicts.size} merge conflicts before requesting a review.`
      );
      return;
    }

    let filteredEntries = Array.from(this.state.changes.entries()).filter(([key, value]) =>
      this.state.pathToIncludeChanges.get(key)
    );
    let filenames = filteredEntries.map(([key, value]) => key).join(", ");
    this.updateState({
      prTitle: `Update ${filenames}`,
      prBody: `Update ${filenames}\n\nBuilt with [BuildBuddy Code](https://${window.location.host}/code/)`,
      reviewRequestModalVisible: true,
    });
  }

  async handleReviewClicked() {
    if (!this.props.user.githubToken) {
      this.handleGitHubClicked();
      return;
    }

    this.updateState({ requestingReview: true });

    let filteredEntries = Array.from(this.state.changes.entries()).filter(
      ([key, value]) => this.state.pathToIncludeChanges.get(key) // Only include checked changes
    );

    let response = await createPullRequest(this.octokit, {
      owner: this.currentOwner(),
      repo: this.currentRepo(),
      title: this.state.prTitle,
      body: this.state.prBody,
      head: `change-${Math.floor(Math.random() * 10000)}`,
      changes: [
        {
          files: Object.fromEntries(
            filteredEntries.map(([key, value]) => [key, { content: btoa(value), encoding: "base64" }]) // Convert to base64 for github to support utf-8
          ),
          commit: this.state.prBody,
        },
      ],
    });

    this.updateState({
      requestingReview: false,
      reviewRequestModalVisible: false,
      prLink: response.data.html_url,
      prNumber: response.data.number,
      prBranch: response.data.head.ref,
    });

    window.open(response.data.html_url, "_blank");

    console.log(response);
  }

  handleChangeClicked(fullPath: string) {
    this.navigateToPath(fullPath);
    this.editor.setModel(this.state.fullPathToModelMap.get(fullPath));
  }

  handleCheckboxClicked(fullPath: string) {
    this.state.pathToIncludeChanges.set(fullPath, !this.state.pathToIncludeChanges.get(fullPath));
    this.updateState({ pathToIncludeChanges: this.state.pathToIncludeChanges });
  }

  // TODO(siggisim): Implement delete
  handleDeleteClicked(fullPath: string) {
    alert("Delete not yet implemented!");
  }

  handleNewFileClicked() {
    let fileName = prompt("File name:");
    if (fileName) {
      let fileContents = "// Your code here";
      let model = this.state.fullPathToModelMap.get(fileName);
      if (!model) {
        model = monaco.editor.createModel(fileContents, undefined, monaco.Uri.file(fileName));
        this.state.fullPathToModelMap.set(fileName, model);
      }
      this.state.originalFileContents.set(fileName, "");
      this.navigateToPath(fileName);
      this.updateState(
        {
          originalFileContents: this.state.originalFileContents,
          changes: this.state.changes,
        },
        () => {
          this.editor.setModel(model);
          this.handleContentChanged();
        }
      );
    }
  }

  handleGitHubClicked() {
    const params = new URLSearchParams({
      user_id: this.props.user?.displayUser?.userId.id,
      redirect_url: window.location.href,
    });
    window.location.href = `/auth/github/link/?${params}`;
  }

  handleUpdatePR() {
    if (!this.props.user.githubToken) {
      this.handleGitHubClicked();
      return;
    }

    this.updateState({ updatingPR: true });

    let filteredEntries = Array.from(this.state.changes.entries()).filter(
      ([key, value]) => this.state.pathToIncludeChanges.get(key) // Only include checked changes
    );

    let filenames = filteredEntries.map(([key, value]) => key).join(", ");

    updatePullRequest(this.octokit, {
      owner: this.currentOwner(),
      repo: this.currentRepo(),
      head: this.state.prBranch,
      changes: [
        {
          files: Object.fromEntries(
            filteredEntries.map(([key, value]) => [key, { content: btoa(value), encoding: "base64" }]) // Convert to base64 for github to support utf-8
          ),
          commit: `Update of ${filenames} using BuildBuddy Code`,
        },
      ],
    }).then(() => {
      this.updateState({ updatingPR: false });
      window.open(this.state.prLink, "_blank");
    });
  }

  handleClearPRClicked() {
    this.updateState({
      prNumber: "",
      prLink: "",
      prBranch: "",
    });
  }

  handleMergePRClicked() {
    this.octokit.rest.pulls
      .merge({
        owner: this.currentOwner(),
        repo: this.currentRepo(),
        pull_number: Number(this.state.prNumber),
      })
      .then(() => {
        window.open(this.state.prLink, "_blank");
        this.handleClearPRClicked();
      });
  }

  handleRevertClicked(path: string, event: MouseEvent) {
    this.state.changes.delete(path);
    this.state.fullPathToModelMap.get(path).setValue(this.state.originalFileContents.get(path));
    this.updateState({ changes: this.state.changes, fullPathToModelMap: this.state.fullPathToModelMap });
    event.stopPropagation();
  }

  // If a callback is set, alert messages will not be shown.
  handleUpdateCommitSha(callback?: (conflicts: number) => void) {
    this.octokit
      .request(`/repos/${this.currentOwner()}/${this.currentRepo()}/compare/${this.state.commitSHA}...master`)
      .then((response: any) => {
        console.log(response);
        let newCommits = response.data.ahead_by;
        let newSha = response.data.commits.pop()?.sha;
        if (newCommits == 0) {
          if (callback) {
            callback(0);
          } else {
            alert_service.success(`You're already up to date!`);
          }
          return;
        }

        let conflictCount = 0;
        for (let file of response.data.files) {
          if (this.state.changes.has(file.filename)) {
            this.state.mergeConflicts.set(file.filename, file.sha);
            conflictCount++;
          }
        }

        this.octokit
          .request(`/repos/${this.currentOwner()}/${this.currentRepo()}/git/trees/${newSha}`)
          .then((response: any) => {
            console.log(response);
            this.updateState(
              { repoResponse: response, mergeConflicts: this.state.mergeConflicts, commitSHA: newSha },
              () => {
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
                    this.state.mergeConflicts.get(this.currentPath()),
                    undefined
                  );
                }
              }
            );
          });
      });
  }

  onTitleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const input = e.target;
    this.updateState({ prTitle: input.value });
  }

  onBodyChange(e: React.ChangeEvent<HTMLInputElement>) {
    const input = e.target;
    this.updateState({ prBody: input.value });
  }

  handleResolveClicked(fullPath: string, sha: string, event: MouseEvent) {
    event.stopPropagation();
    this.state.changes.set(fullPath, this.state.fullPathToDiffModelMap.get(fullPath).modified.getValue());
    this.state.fullPathToDiffModelMap.delete(fullPath);
    this.state.mergeConflicts.delete(fullPath);
    this.updateState({
      changes: this.state.changes,
      fullPathToDiffModelMap: this.state.fullPathToDiffModelMap,
      mergeConflicts: this.state.mergeConflicts,
    });
  }

  handleViewConflictClicked(fullPath: string, sha: string, event: MouseEvent) {
    event?.stopPropagation();
    this.octokit.rest.git
      .getBlob({
        owner: this.currentOwner(),
        repo: this.currentRepo(),
        file_sha: sha,
      })
      .then((response: any) => {
        console.log(response);
        if (!this.diffEditor) {
          this.diffEditor = monaco.editor.createDiffEditor(this.diffViewer.current);
        }
        let fileContents = atob(response.data.content);
        let editedModel = this.state.fullPathToModelMap.get(fullPath);
        let uri = monaco.Uri.file(`${fullPath}-${sha}`);
        let latestModel = monaco.editor.getModel(uri);
        if (!latestModel) {
          latestModel = monaco.editor.createModel(fileContents, undefined, uri);
        }
        let diffModel = { original: latestModel, modified: editedModel };
        this.diffEditor.setModel(diffModel);
        this.state.fullPathToDiffModelMap.set(fullPath, diffModel);

        this.navigateToPath(fullPath);
        this.updateState({ fullPathToDiffModelMap: this.state.fullPathToDiffModelMap }, () => {
          this.diffEditor.layout();
        });
      });
  }

  handleCloseReviewModal() {
    this.updateState({ reviewRequestModalVisible: false });
  }

  updateState(newState: Partial<State>, callback?: VoidFunction) {
    this.setState(newState as State, () => {
      this.saveState();
      if (callback) {
        callback();
      }
    });
  }

  // TODO(siggisim): Make the menu look nice
  // TODO(siggisim): Make sidebar look nice
  // TODO(siggisim): Make the diff view look nicer
  render() {
    if (!this.currentRepo() && !this.isSingleFile()) {
      return <CodeEmptyStateComponent />;
    }

    setTimeout(() => {
      this.editor?.layout();
    }, 0);

    let showDiffView = this.state.fullPathToDiffModelMap.has(this.currentPath());
    return (
      <div className="code-editor">
        <div className="code-menu">
          <div className="code-menu-logo">
            {this.isSingleFile() && (
              <a href="javascript:history.back()">
                <ArrowLeft className="code-menu-back" />
              </a>
            )}
            <a href="/">
              <img alt="BuildBuddy Code" src="/image/logo_dark.svg" className="logo" /> Code{" "}
              <Code className="icon code-logo" />
            </a>
          </div>
          <div className="code-menu-breadcrumbs">
            {this.isSingleFile() && (
              <div className="code-menu-breadcrumbs-filename">{this.props.search.get("filename")}</div>
              // todo show hash and size
              // todo show warning message for files larger than ~50mb
            )}
            {!this.isSingleFile() && (
              <>
                <div className="code-menu-breadcrumbs-environment">
                  {/* <a href="#">my-workspace</a> /{" "} TODO: add workspace to breadcrumb */}
                  <a target="_blank" href={`http://github.com/${this.currentOwner()}`}>
                    {this.currentOwner()}
                  </a>{" "}
                  /{" "}
                  <a target="_blank" href={`http://github.com/${this.currentOwner()}/${this.currentRepo()}`}>
                    {this.currentRepo()}
                  </a>{" "}
                  {/* <a href="#">master</a> / TODO: add branch to breadcrumb  */}@{" "}
                  <a
                    target="_blank"
                    title={this.state.commitSHA}
                    href={`http://github.com/${this.currentOwner()}/${this.currentRepo()}/commit/${
                      this.state.commitSHA
                    }`}>
                    {this.state.commitSHA?.slice(0, 7)}
                  </a>{" "}
                  <span onClick={this.handleUpdateCommitSha.bind(this, undefined)}>
                    <ArrowUpCircle className="code-update-commit" />
                  </span>{" "}
                </div>
                <div className="code-menu-breadcrumbs-filename">
                  {this.currentPath() ? (
                    <>
                      <a href="#">{this.currentPath()}</a>
                    </>
                  ) : undefined}
                </div>
              </>
            )}
          </div>
          {this.isSingleFile() && (
            <div className="code-menu-actions">
              <OutlinedButton
                className="code-menu-download-button"
                onClick={() =>
                  rpcService.downloadBytestreamFile(
                    this.props.search.get("filename"),
                    this.props.search.get("bytestream_url"),
                    this.props.search.get("invocation_id")
                  )
                }>
                <Download /> Download File
              </OutlinedButton>
            </div>
          )}
          {!this.isSingleFile() && (
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
                isLoading={this.state.isBuilding}
                project={`${this.currentOwner()}/${this.currentRepo()}}`}
              />
            </div>
          )}
        </div>
        <div className="code-main">
          {!this.isSingleFile() && (
            <div className="code-sidebar">
              <div className="code-sidebar-tree">
                {this.state.repoResponse &&
                  this.state.repoResponse.data.tree
                    .sort(compareNodes)
                    .map((node: any) => (
                      <SidebarNodeComponent
                        node={node}
                        treeShaToExpanded={this.state.treeShaToExpanded}
                        treeShaToChildrenMap={this.state.treeShaToChildrenMap}
                        handleFileClicked={this.handleFileClicked.bind(this)}
                        fullPath={node.path}
                      />
                    ))}
              </div>
              <div className="code-sidebar-actions">
                {!this.props.user.githubToken && (
                  <button onClick={this.handleGitHubClicked.bind(this)}>
                    <Link className="icon" /> Link GitHub
                  </button>
                )}
                <button onClick={this.handleNewFileClicked.bind(this)}>
                  <PlusCircle className="icon green" /> New
                </button>
                <button onClick={this.handleDeleteClicked.bind(this)}>
                  <XCircle className="icon red" /> Delete
                </button>
              </div>
            </div>
          )}
          <div className="code-container">
            <div className="code-viewer-container">
              <div className={`code-viewer ${showDiffView ? "hidden-viewer" : ""}`} ref={this.codeViewer} />
              <div className={`diff-viewer ${showDiffView ? "" : "hidden-viewer"}`} ref={this.diffViewer} />
            </div>
            {this.state.changes.size > 0 && (
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
                {Array.from(this.state.changes.keys()).map((fullPath) => (
                  <div className="code-diff-viewer-item" onClick={() => this.handleChangeClicked(fullPath)}>
                    <input
                      checked={this.state.pathToIncludeChanges.get(fullPath)}
                      onChange={(event) => this.handleCheckboxClicked(fullPath)}
                      type="checkbox"
                    />{" "}
                    {fullPath}
                    {this.state.mergeConflicts.has(fullPath) && fullPath != this.currentPath() && (
                      <span
                        className="code-revert-button"
                        onClick={this.handleViewConflictClicked.bind(
                          this,
                          fullPath,
                          this.state.mergeConflicts.get(fullPath)
                        )}>
                        View Conflict
                      </span>
                    )}
                    {this.state.mergeConflicts.has(fullPath) && fullPath == this.currentPath() && (
                      <span
                        className="code-revert-button"
                        onClick={this.handleResolveClicked.bind(
                          this,
                          fullPath,
                          this.state.mergeConflicts.get(fullPath)
                        )}>
                        Resolve Conflict
                      </span>
                    )}
                    <span className="code-revert-button" onClick={this.handleRevertClicked.bind(this, fullPath)}>
                      Revert
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
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
                <FilledButton
                  disabled={this.state.requestingReview}
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
        baseUrl: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.25.2/min/'
      };
      importScripts('https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.25.2/min/vs/base/worker/workerMain.js');`)}`;
  },
};

// This replaces any non-serializable objects in state with serializable models.
function stateReplacer(key: any, value: any) {
  if (key == "fullPathToModelMap") {
    return {
      dataType: "ModelMap",
      value: Array.from(value.entries()).map((e: any) => {
        return {
          dataType: "Model",
          key: e[0],
          value: e[1].getValue(),
          uri: e[1].uri,
        };
      }),
    };
  }
  if (key == "fullPathToDiffModelMap") {
    return {
      dataType: "DiffModelMap",
      value: Array.from(value.entries()).map((e: any) => {
        return {
          dataType: "Model",
          key: e[0],
          original: e[1].original.getValue(),
          modified: e[1].modified.getValue(),
          originalUri: e[1].original.uri,
          modifiedUri: e[1].modified.uri,
        };
      }),
    };
  }
  if (value instanceof Map) {
    return {
      dataType: "Map",
      value: Array.from(value.entries()),
    };
  }
  return value;
}

// This revives any non-serializable objects in state from their seralized form.
function stateReviver(key: any, value: any) {
  if (typeof value === "object" && value !== null) {
    if (value.dataType === "Map") {
      return new Map(value.value);
    }
    if (value.dataType === "ModelMap") {
      return new Map(value.value.map((e: any) => [e.key, monaco.editor.createModel(e.value, undefined, e.uri.path)]));
    }
    if (value.dataType === "DiffModelMap") {
      return new Map(
        value.value.map((e: any) => [
          e.key,
          {
            original: monaco.editor.createModel(e.original, undefined, e.originalUri.path),
            modified: monaco.editor.createModel(e.modified, undefined, e.modifiedUri.path),
          },
        ])
      );
    }
  }
  return value;
}
