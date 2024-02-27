import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import SidebarNodeComponent, { compareNodes } from "./code_sidebar_node";
import { Subscription } from "rxjs";
import * as monaco from "monaco-editor";
import * as diff from "diff";
import { runner } from "../../../proto/runner_ts_proto";
import CodeBuildButton from "./code_build_button";
import CodeEmptyStateComponent from "./code_empty";
import { ArrowLeft, ArrowUpCircle, ChevronRight, Download, Key, Link, PlusCircle, Send, XCircle } from "lucide-react";
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
import { parseLcov } from "../../../app/util/lcov";
import { github } from "../../../proto/github_ts_proto";
import Long from "long";
import ModuleSidekick from "../sidekick/module/module";
import BazelVersionSidekick from "../sidekick/bazelversion/bazelversion";
import BazelrcSidekick from "../sidekick/bazelrc/bazelrc";
import error_service from "../../../app/errors/error_service";

interface Props {
  user: User;
  tab: string;
  path: string;
  search: URLSearchParams;
}
interface State {
  commitSHA: string;
  repoResponse: github.GetGithubRepoResponse | undefined;
  treeResponse: github.GetGithubTreeResponse | undefined;
  installationsResponse: github.GetGithubUserInstallationsResponse | undefined;
  fullPathToExpanded: Map<string, boolean>;
  fullPathToRenaming: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, github.TreeNode[]>;
  fullPathToModelMap: Map<string, monaco.editor.ITextModel>;
  fullPathToDiffModelMap: Map<string, monaco.editor.IDiffEditorModel>;
  originalFileContents: Map<string, string>;
  tabs: Map<string, string>;
  /** Map of file path to patch contents, or null if the file is being deleted. */
  changes: Map<string, string | null>;
  temporaryFiles: Map<string, github.TreeNode[]>;
  mergeConflicts: Map<string, string>;
  pathToIncludeChanges: Map<string, boolean>;
  prLink: string;
  prNumber: Long;
  prBranch: string;
  prTitle: string;
  prBody: string;

  requestingReview: boolean;
  updatingPR: boolean;
  reviewRequestModalVisible: boolean;
  isBuilding: boolean;

  showContextMenu?: boolean;
  contextMenuX?: number;
  contextMenuY?: number;
  contextMenuFullPath?: string;
  contextMenuFile?: github.TreeNode;
}

const LOCAL_STORAGE_STATE_KEY = "code-state-v1";
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

// TODO(siggisim): Add links to the code editor from anywhere we reference a repo
// TODO(siggisim): Add branch / workspace selection
// TODO(siggisim): Add some form of search
export default class CodeComponent extends React.Component<Props, State> {
  state: State = {
    commitSHA: "",
    repoResponse: undefined,
    treeResponse: undefined,
    installationsResponse: undefined,
    fullPathToExpanded: new Map<string, boolean>(),
    fullPathToRenaming: new Map<string, boolean>(),
    treeShaToChildrenMap: new Map<string, Array<github.TreeNode>>(),
    fullPathToModelMap: new Map<string, monaco.editor.ITextModel>(),
    fullPathToDiffModelMap: new Map<string, monaco.editor.IDiffEditorModel>(),
    originalFileContents: new Map<string, string>(),
    tabs: new Map<string, string>(),
    changes: new Map<string, string>(),
    temporaryFiles: new Map<string, github.TreeNode[]>(),
    mergeConflicts: new Map<string, string>(),
    pathToIncludeChanges: new Map<string, boolean>(),
    prLink: "",
    prBranch: "",
    prNumber: new Long(0),

    requestingReview: false,
    updatingPR: false,
    reviewRequestModalVisible: false,
    isBuilding: false,

    prTitle: "",
    prBody: "",
  };

  editor: monaco.editor.IStandaloneCodeEditor | undefined;
  diffEditor: monaco.editor.IDiffEditor | undefined;

  codeViewer = React.createRef<HTMLDivElement>();
  diffViewer = React.createRef<HTMLDivElement>();

  subscription?: Subscription;
  fetchedInitialContent = false;

  componentWillMount() {
    let githubUrl = this.props.search.get("github_url");
    if (githubUrl) {
      window.location.href = this.parseGithubUrl(githubUrl);
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

  isSingleFile() {
    return Boolean(this.props.search.get("bytestream_url")) || Boolean(this.props.search.get("lcov"));
  }

  componentDidMount() {
    if (this.state.repoResponse || this.isSingleFile()) {
      this.fetchInitialContent();
    }

    document.onkeydown = (e) => {
      switch (e.keyCode) {
        case 70: // Meta + F
          if (!e.metaKey) break;
          this.editor?.focus();
          this.editor?.trigger("find", "editor.actions.findWithArgs", { searchString: "" });
          e.preventDefault();
          break;
      }
    };
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
    }
    return destinationUrl;
  }

  fetchContentForPath(path: string) {
    return rpcService.service.getGithubContent(
      new github.GetGithubContentRequest({
        owner: this.currentOwner(),
        repo: this.currentRepo(),
        path: path,
        ref: this.getRef(),
      })
    );
  }

  getRef() {
    return this.state.commitSHA || this.getBranch();
  }

  getBranch() {
    return this.props.search.get("branch") || this.state.repoResponse?.defaultBranch;
  }

  fetchInitialContent() {
    if (this.fetchedInitialContent) {
      return;
    }
    this.fetchedInitialContent = true;

    if (!this.currentRepo() && !this.isSingleFile()) {
      return;
    }

    window.addEventListener("resize", () => this.handleWindowResize());

    this.editor = monaco.editor.create(this.codeViewer.current!, {
      value: ["// Welcome to BuildBuddy Code!", "", "// Click on a file to the left to get start editing."].join("\n"),
      theme: "vs",
      readOnly: this.isSingleFile(),
    });
    this.forceUpdate();

    const bytestreamURL = this.props.search.get("bytestream_url") || "";
    const invocationID = this.props.search.get("invocation_id") || "";
    const zip = this.props.search.get("z") || undefined;
    let filename = this.props.search.get("filename");
    if (this.isSingleFile() && bytestreamURL) {
      rpcService.fetchBytestreamFile(bytestreamURL, invocationID, "text", { zip }).then((result) => {
        let path = monaco.Uri.file(filename || "file");
        this.setModel(path.path, monaco.editor.createModel(result, langFromPath(path.path), path));
      });
      return;
    }

    const lcovURL = this.props.search.get("lcov");
    const commit = this.props.search.get("commit");
    if (this.isSingleFile() && lcovURL) {
      rpcService.service
        .getGithubContent(
          new github.GetGithubContentRequest({
            owner: this.currentOwner(),
            repo: this.currentRepo(),
            path: this.currentPath(),
            ref: commit || this.getRef(),
          })
        )
        .then((response) => {
          this.navigateToContent(this.currentPath(), response.content);
          rpcService.fetchBytestreamFile(lcovURL, invocationID, "text").then((result) => {
            let records = parseLcov(result);
            for (let record of records) {
              if (record.sourceFile == this.currentPath()) {
                this.editor?.deltaDecorations(
                  [],
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
        });
      return;
    }

    if (this.currentPath()) {
      if (!this.state.fullPathToModelMap.has(this.currentPath())) {
        this.fetchContentForPath(this.currentPath()).then((response) => {
          this.navigateToContent(this.currentPath(), response.content);
        });
      }

      if (this.state.mergeConflicts.has(this.currentPath())) {
        this.handleViewConflictClicked(
          this.currentPath(),
          this.state.mergeConflicts.get(this.currentPath())!,
          undefined
        );
      } else {
        this.editor.setModel(this.state.fullPathToModelMap.get(this.currentPath()) || null);
      }
    }

    this.editor.onDidChangeModelContent(() => {
      this.handleContentChanged();
    });
  }

  handleContentChanged() {
    if (this.state.originalFileContents.get(this.currentPath()) === this.editor?.getValue()) {
      this.state.changes.delete(this.currentPath());
      this.state.pathToIncludeChanges.delete(this.currentPath());
    } else if (this.currentPath()) {
      if (!this.state.changes.get(this.currentPath())) {
        this.state.pathToIncludeChanges.set(this.currentPath(), true);
      }
      this.state.changes.set(this.currentPath(), this.editor?.getValue() || "");
    }
    this.updateState({ changes: this.state.changes });
  }

  saveState() {
    // TODO(siggisim): store this in cache.
    localStorage.setItem(this.getStateCacheKey(), JSON.stringify(this.state, stateReplacer));
  }

  getStateCacheKey() {
    return `${LOCAL_STORAGE_STATE_KEY}/${this.currentOwner()}/${this.currentRepo()}/${this.getBranch()}`;
  }

  componentWillUnmount() {
    window.removeEventListener("resize", () => this.handleWindowResize());
    this.subscription?.unsubscribe();
    this.editor?.dispose();
  }

  componentDidUpdate(prevProps: Props, prevState: State) {
    if (!prevState.repoResponse && this.state.repoResponse) {
      this.fetchInitialContent();
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

  async fetchCode() {
    const storageValue = localStorage.getItem(this.getStateCacheKey());
    const storedState = storageValue ? (JSON.parse(storageValue, stateReviver) as State) : undefined;
    if (storedState) {
      this.setState(storedState);
      if (storedState.treeResponse && storedState.commitSHA) {
        return;
      }
    }
    if (!this.currentOwner() || !this.currentRepo()) {
      return;
    }

    let repoResponse = await rpcService.service.getGithubRepo(
      new github.GetGithubRepoRequest({ owner: this.currentOwner(), repo: this.currentRepo() })
    );
    console.log(repoResponse);

    rpcService.service
      .getGithubTree(
        new github.GetGithubTreeRequest({
          owner: this.currentOwner(),
          repo: this.currentRepo(),
          ref: this.getRef() || repoResponse.defaultBranch,
        })
      )
      .then((treeResponse) => {
        console.log(treeResponse);
        this.updateState({ repoResponse: repoResponse, treeResponse: treeResponse, commitSHA: treeResponse.sha });
      });
  }

  isNewFile(node: github.TreeNode) {
    return !node.sha;
  }

  isDirectory(node: github.TreeNode | undefined) {
    return node?.type === "tree";
  }

  // TODO(siggisim): Support moving files around
  handleFileClicked(node: github.TreeNode, fullPath: string) {
    if (this.isNewFile(node)) {
      if (this.isDirectory(node)) {
        this.state.fullPathToExpanded.set(fullPath, !Boolean(this.state.fullPathToExpanded.get(fullPath)));
        this.state.treeShaToChildrenMap.set(node.sha, []);
      } else {
        this.navigateToPath(fullPath);
        this.setModel(fullPath, this.state.fullPathToModelMap.get(fullPath));
      }
      this.updateState({});
      return;
    }

    if (this.isDirectory(node)) {
      if (this.state.fullPathToExpanded.get(fullPath)) {
        this.state.fullPathToExpanded.set(fullPath, false);
        this.updateState({ fullPathToExpanded: this.state.fullPathToExpanded });
        return;
      }

      rpcService.service
        .getGithubTree(
          new github.GetGithubTreeRequest({
            owner: this.currentOwner(),
            repo: this.currentRepo(),
            ref: node.sha,
          })
        )
        .then((response) => {
          this.state.fullPathToExpanded.set(fullPath, true);
          this.state.treeShaToChildrenMap.set(node.sha, response.nodes);
          this.updateState({
            treeShaToChildrenMap: this.state.treeShaToChildrenMap,
          });
          console.log(response);
        });
      return;
    }

    rpcService.service
      .getGithubBlob(
        new github.GetGithubBlobRequest({
          owner: this.currentOwner(),
          repo: this.currentRepo(),
          sha: node.sha,
        })
      )
      .then((response) => {
        console.log(response);
        this.navigateToContent(fullPath, response.content);
        this.navigateToPath(fullPath);
      });
  }

  ensureModelExists(fullPath: string, content: Uint8Array) {
    let fileContents = textDecoder.decode(content);
    this.state.originalFileContents.set(fullPath, fileContents);
    this.updateState({
      originalFileContents: this.state.originalFileContents,
      changes: this.state.changes,
    });
    let model = this.state.fullPathToModelMap.get(fullPath);
    if (!model) {
      model = monaco.editor.createModel(fileContents, langFromPath(fullPath), monaco.Uri.file(fullPath));
      this.state.fullPathToModelMap.set(fullPath, model);
      this.updateState({ fullPathToModelMap: this.state.fullPathToModelMap });
    }
    return model;
  }

  handleTabClicked(fullPath: string) {
    this.navigateToPath(fullPath);
    this.setModel(fullPath, this.state.fullPathToModelMap.get(fullPath));
  }

  navigateToContent(fullPath: string, content: Uint8Array) {
    this.setModel(fullPath, this.ensureModelExists(fullPath, content));
  }

  setModel(fullPath: string, model: monaco.editor.ITextModel | undefined) {
    this.state.tabs.set(fullPath, fullPath);
    this.editor?.setModel(model || null);
    this.updateState({ tabs: this.state.tabs });
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
    request.async = true;

    this.updateState({ isBuilding: true });
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

  getRepoState() {
    let state = new runner.RunRequest.RepoState();
    state.commitSha = this.state.commitSHA;
    state.branch = this.getBranch()!;
    for (let path of this.state.changes.keys()) {
      state.patch.push(
        textEncoder.encode(
          diff.createTwoFilesPatch(
            `a/${path}`,
            `b/${path}`,
            this.state.originalFileContents.get(path) || "",
            this.state.changes.get(path) || ""
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
      this.state.pathToIncludeChanges.get(key)
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
      ([key, value]) => this.state.pathToIncludeChanges.get(key) // Only include checked changes
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
            filteredEntries.map(([key, value]) => (value ? [key, { content: textEncoder.encode(value) }] : [key, null]))
          ),
          commit: this.state.prBody,
        },
      ],
    })
      .then((response) => {
        this.updateState({
          requestingReview: false,
          reviewRequestModalVisible: false,
          prLink: response.url,
          prNumber: response.pullNumber,
          prBranch: response.ref,
        });
        window.open(response.url, "_blank");
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
    if (this.state.changes.get(fullPath) === null) {
      return;
    }
    this.navigateToPath(fullPath);
    this.setModel(fullPath, this.state.fullPathToModelMap.get(fullPath));
  }

  handleCheckboxClicked(fullPath: string) {
    this.state.pathToIncludeChanges.set(fullPath, !this.state.pathToIncludeChanges.get(fullPath));
    this.updateState({ pathToIncludeChanges: this.state.pathToIncludeChanges });
  }

  handleDeleteClicked(fullPath: string, node: github.TreeNode) {
    if (this.isDirectory(node)) {
      error_service.handleError("Deleting directories is not yet supported");
      return;
    }
    if (this.isNewFile(node)) {
      this.state.changes.delete(fullPath);
      this.state.pathToIncludeChanges.delete(fullPath);
    } else {
      this.state.changes.set(fullPath, null);
      this.state.pathToIncludeChanges.set(fullPath, true);
    }

    this.updateState({ changes: this.state.changes });
  }

  newFileWithContents(node: github.TreeNode, path: string, contents: string) {
    let parent = this.getParent(path);
    let tempFiles = this.state.temporaryFiles.get(parent);
    if (tempFiles) {
      tempFiles.splice(tempFiles.indexOf(node), 1);
      this.state.temporaryFiles.set(parent, tempFiles);
    }

    let model = this.state.fullPathToModelMap.get(path);
    if (!model) {
      model = monaco.editor.createModel(contents, langFromPath(path), monaco.Uri.file(path));
      this.state.fullPathToModelMap.set(path, model);
    }
    this.navigateToPath(path);
    this.updateState({}, () => {
      this.setModel(path, model);
      this.handleContentChanged();
    });
  }

  handleRenameClicked(fullPath: string) {
    this.state.fullPathToRenaming.set(fullPath, true);
    this.updateState({});
  }

  handleNewFileClicked(path: string) {
    if (!this.state.temporaryFiles.has(path)) {
      this.state.temporaryFiles.set(path, []);
    }
    this.state.temporaryFiles.get(path)!.push(new github.TreeNode({ path: "", type: "file" }));
    this.updateState({ temporaryFiles: this.state.temporaryFiles });
  }

  handleNewFolderClicked(path: string) {
    if (!this.state.temporaryFiles.has(path)) {
      this.state.temporaryFiles.set(path, []);
    }
    this.state.temporaryFiles.get(path)!.push(new github.TreeNode({ path: "", type: "tree" }));
    this.updateState({ temporaryFiles: this.state.temporaryFiles });
  }

  handleGitHubClicked() {
    const params = new URLSearchParams({
      redirect_url: window.location.href,
      ...(this.props.user.displayUser.userId && { user_id: this.props.user.displayUser.userId.id }),
    });
    window.location.href = `/auth/github/link/?${params}`;
  }

  handleUpdatePR() {
    if (!this.props.user.githubLinked) {
      this.handleGitHubClicked();
      return;
    }

    this.updateState({ updatingPR: true });

    let filteredEntries = Array.from(this.state.changes.entries()).filter(
      ([key, value]) => this.state.pathToIncludeChanges.get(key) // Only include checked changes
    );

    let filenames = filteredEntries.map(([key, value]) => key).join(", ");

    updatePullRequest({
      owner: this.currentOwner(),
      repo: this.currentRepo(),
      head: this.state.prBranch,
      changes: [
        {
          files: Object.fromEntries(
            filteredEntries.map(([key, value]) => (value ? [key, { content: textEncoder.encode(value) }] : [key, null]))
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

  handleRevertClicked(path: string, event: React.MouseEvent<HTMLSpanElement, MouseEvent>) {
    this.state.changes.delete(path);
    if (this.state.originalFileContents.has(path)) {
      this.state.fullPathToModelMap.get(path)?.setValue(this.state.originalFileContents.get(path) || "");
    }
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
          base: this.state.commitSHA,
          head: this.getBranch(),
        })
      )
      .then(async (response) => {
        console.log(response);
        let newCommits = response.aheadBy;
        let newSha = response.commits.pop()?.sha;
        if (Number(newCommits) == 0) {
          if (callback) {
            callback(0);
          } else {
            alert_service.success(`You're already up to date!`);
          }
          return;
        }

        let conflictCount = 0;
        for (let file of response.files) {
          if (this.state.changes.has(file.name)) {
            let response = await rpcService.service.getGithubContent(
              new github.GetGithubContentRequest({
                owner: this.currentOwner(),
                repo: this.currentRepo(),
                path: this.currentPath(),
                ref: newSha,
              })
            );
            let newFileContent = textDecoder.decode(response.content);
            this.state.originalFileContents.set(file.name, newFileContent);
            if (newFileContent == this.state.changes.get(file.name)) {
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
          .getGithubTree(
            new github.GetGithubTreeRequest({
              owner: this.currentOwner(),
              repo: this.currentRepo(),
              ref: newSha,
            })
          )
          .then((response) => {
            console.log(response);
            this.updateState(
              { treeResponse: response, mergeConflicts: this.state.mergeConflicts, commitSHA: newSha },
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
                    this.state.mergeConflicts.get(this.currentPath())!,
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

  onBodyChange(e: React.ChangeEvent<HTMLTextAreaElement>) {
    const input = e.target;
    this.updateState({ prBody: input.value });
  }

  handleResolveClicked(fullPath: string, event: React.MouseEvent<HTMLSpanElement, MouseEvent>) {
    event.stopPropagation();
    this.state.changes.set(fullPath, this.state.fullPathToDiffModelMap.get(fullPath)?.modified.getValue() || "");
    this.state.fullPathToDiffModelMap.delete(fullPath);
    this.state.mergeConflicts.delete(fullPath);
    this.updateState({
      changes: this.state.changes,
      fullPathToDiffModelMap: this.state.fullPathToDiffModelMap,
      mergeConflicts: this.state.mergeConflicts,
    });
  }

  handleViewConflictClicked(fullPath: string, sha: string, event?: React.MouseEvent<HTMLSpanElement, MouseEvent>) {
    event?.stopPropagation();
    rpcService.service
      .getGithubBlob(
        new github.GetGithubBlobRequest({
          owner: this.currentOwner(),
          repo: this.currentRepo(),
          sha: sha,
        })
      )
      .then((response) => {
        console.log(response);
        if (!this.diffEditor) {
          this.diffEditor = monaco.editor.createDiffEditor(this.diffViewer.current!);
        }
        let fileContents = textDecoder.decode(response.content);
        let editedModel = this.state.fullPathToModelMap.get(fullPath)!;
        let uri = monaco.Uri.file(`${fullPath}-${sha}`);
        let latestModel = monaco.editor.getModel(uri);
        if (!latestModel) {
          latestModel = monaco.editor.createModel(fileContents, langFromPath(uri.path), uri);
        }
        let diffModel = { original: latestModel, modified: editedModel };
        this.diffEditor.setModel(diffModel);
        this.state.fullPathToDiffModelMap.set(fullPath, diffModel);

        this.navigateToPath(fullPath);
        this.updateState({ fullPathToDiffModelMap: this.state.fullPathToDiffModelMap }, () => {
          this.diffEditor?.layout();
        });
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
    return paths.filter((p) => p).join("");
  }

  async handleRename(node: github.TreeNode, path: string, newValue: string, existingFile: boolean) {
    if (!newValue) {
      return;
    }
    this.state.fullPathToRenaming.set(path, false);
    this.updateState({});

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

      if (!this.state.fullPathToModelMap.has(path)) {
        await this.fetchContentForPath(path).then((response) => {
          return this.ensureModelExists(path, response.content);
        });
      }

      this.newFileWithContents(node, newValue, this.state.fullPathToModelMap.get(path)?.getValue() || "");
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

  handleContextMenu(node: github.TreeNode | undefined, fullPath: string, event: React.MouseEvent) {
    if (this.isDirectory(node)) {
      this.state.fullPathToExpanded.set(fullPath, true);
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

  updateState(newState: Partial<State>, callback?: VoidFunction) {
    this.setState(newState as State, () => {
      this.saveState();
      if (callback) {
        callback();
      }
    });
  }

  getFiles(nodes: github.TreeNode[], parent: string) {
    // Add any temporary files
    let files = new Map<string, github.TreeNode>();
    for (let t of this.state.temporaryFiles.get(parent) || []) {
      files.set(t.path, t);
    }

    // And any matching changed files, or paths leading up to changed files
    for (let path of this.state.changes.keys()) {
      let matchesRootDirectory = parent == "" && path.indexOf("/") == -1;
      if (path.startsWith(parent + "/") || matchesRootDirectory) {
        let remainder = parent == "" ? path : path.substr(parent.length + 1);
        let dirs = remainder.split("/");
        files.set(dirs[0], new github.TreeNode({ path: dirs[0], type: dirs.length > 1 ? "tree" : "file" }));
      }
    }

    // And finally the original files from github
    for (let o of nodes) {
      files.set(o.path, o);
    }

    return Array.from(files.values());
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
              <img alt="BuildBuddy Code" src="/image/b_dark.svg" className="logo" />
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
                  <ChevronRight />
                  <a target="_blank" href={`http://github.com/${this.currentOwner()}/${this.currentRepo()}`}>
                    {this.currentRepo()}
                  </a>
                </div>
                <ChevronRight />
                {this.state.commitSHA != this.getBranch() && (
                  <>
                    <a
                      href={`http://github.com/${this.currentOwner()}/${this.currentRepo()}/tree/${
                        this.state.prBranch || this.getBranch()
                      }`}>
                      {this.state.prBranch || this.getBranch()}
                    </a>
                    <ChevronRight />
                  </>
                )}
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
                {this.currentPath() && (
                  <>
                    <ChevronRight />
                    <div className="code-menu-breadcrumbs-filename">
                      <a href="#">{this.currentPath()}</a>
                    </div>
                  </>
                )}
              </>
            )}
          </div>
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
              <div className="code-sidebar-tree" onContextMenu={(e) => this.handleContextMenu(undefined, "", e)}>
                {this.state.treeResponse &&
                  this.getFiles(this.state.treeResponse.nodes, "")
                    .sort(compareNodes)
                    .map((node) => (
                      <SidebarNodeComponent
                        node={node}
                        getFiles={this.getFiles.bind(this)}
                        changes={this.state.changes}
                        fullPathToExpanded={this.state.fullPathToExpanded}
                        fullPathToRenaming={this.state.fullPathToRenaming}
                        treeShaToChildrenMap={this.state.treeShaToChildrenMap}
                        handleFileClicked={this.handleFileClicked.bind(this)}
                        fullPath={node.path}
                        handleContextMenu={this.handleContextMenu.bind(this)}
                        handleRename={this.handleRename.bind(this)}
                      />
                    ))}
              </div>
              {!this.props.user.githubLinked && (
                <div className="code-sidebar-actions">
                  <button onClick={this.handleGitHubClicked.bind(this)}>
                    <Link className="icon" /> Link GitHub
                  </button>
                </div>
              )}
            </div>
          )}
          <div className="code-container">
            {!this.isSingleFile() && (
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
                    <div
                      className={`code-diff-viewer-item-path${
                        this.state.changes.get(fullPath) === null ? " deleted" : ""
                      }${
                        this.state.originalFileContents.has(fullPath) || this.state.changes.get(fullPath) === null
                          ? ""
                          : " added"
                      }`}>
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
                    <span className="code-revert-button" onClick={this.handleRevertClicked.bind(this, fullPath)}>
                      Revert
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
          {this.editor && (this.currentPath()?.endsWith("MODULE.bazel") || this.currentPath()?.endsWith("MODULE")) && (
            <ModuleSidekick editor={this.editor} />
          )}
          {this.editor && this.currentPath()?.endsWith(".bazelversion") && (
            <BazelVersionSidekick editor={this.editor} />
          )}
          {this.editor && this.currentPath()?.endsWith(".bazelrc") && <BazelrcSidekick editor={this.editor} />}
        </div>
        {this.state.showContextMenu && (
          <div className="context-menu-container">
            <div
              className="context-menu"
              onClick={this.clearContextMenu.bind(this)}
              style={{ top: this.state.contextMenuY, left: this.state.contextMenuX }}>
              <div onClick={() => this.handleNewFileClicked(this.state.contextMenuFullPath!)}>New file</div>
              <div onClick={() => this.handleNewFolderClicked(this.state.contextMenuFullPath!)}>New folder</div>
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
        baseUrl: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.25.2/min/'
      };
      importScripts('https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.25.2/min/vs/base/worker/workerMain.js');`)}`;
  },
};

// This replaces any non-serializable objects in state with serializable models.
function stateReplacer(key: string, value: any) {
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
function stateReviver(key: string, value: any) {
  if (typeof value === "object" && value !== null) {
    if (value.dataType === "Map") {
      return new Map(value.value);
    }
    if (value.dataType === "ModelMap") {
      return new Map(
        value.value.map((e: any) => [e.key, monaco.editor.createModel(e.value, langFromPath(e.key), e.key)])
      );
    }
    if (value.dataType === "DiffModelMap") {
      return new Map(
        value.value.map((e: any) => [
          e.key,
          {
            original: monaco.editor.createModel(e.original, langFromPath(e.originalUri.path), e.originalUri.path),
            modified: monaco.editor.createModel(e.modified, langFromPath(e.modifiedUri.path), e.modifiedUri.path),
          },
        ])
      );
    }
  }
  return value;
}

function langFromPath(path: string) {
  if (!path) {
    return undefined;
  }
  if (
    path.endsWith(".bazel") ||
    path.endsWith("WORKSPACE") ||
    path.endsWith("BUILD") ||
    path.endsWith("MODULE") ||
    path.endsWith(".bzl")
  ) {
    return "python";
  }
  return undefined;
}
