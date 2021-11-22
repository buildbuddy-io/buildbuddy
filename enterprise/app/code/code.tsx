import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import SidebarNodeComponent from "./code_sidebar_node";
import { Subscription } from "rxjs";
import * as monaco from "monaco-editor";
import { Octokit } from "octokit";
import * as diff from "diff";
import { createPullRequest } from "octokit-plugin-create-pull-request";
import { runner } from "../../../proto/runner_ts_proto";

const MyOctokit = Octokit.plugin(createPullRequest);

interface Props {
  user: User;
  hash: string;
  path: string;
  search: URLSearchParams;
}
interface State {
  owner: string;
  repo: string;
  repoResponse: any;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, any[]>;
  treeShaToPathMap: Map<string, string>;
  fullPathToModelMap: Map<string, any>;
  originalFileContents: Map<string, string>;
  currentFilePath: string;
  changes: Map<string, string>;
  pathToIncludeChanges: Map<string, boolean>;

  requestingReview: boolean;
}

const LOCAL_STORAGE_STATE_KEY = "code-state-v1";

// TODO(siggisim): Add links to the code editor from anywhere we reference a repo
// TODO(siggisim): Add branch / workspace selection
// TODO(siggisim): Add currently selected file name
// TODO(siggisim): Add some form of search
export default class CodeComponent extends React.Component<Props> {
  props: Props;

  state: State = {
        owner: "",
        repo: "",
        repoResponse: undefined,
        treeShaToExpanded: new Map<string, boolean>(),
        treeShaToChildrenMap: new Map<string, any[]>(),
        treeShaToPathMap: new Map<string, string>(),
        fullPathToModelMap: new Map<string, any>(),
        originalFileContents: new Map<string, any>(),
        currentFilePath: "",
        changes: new Map<string, string>(),
        pathToIncludeChanges: new Map<string, boolean>(),

        requestingReview: false,
      };

  editor: any;

  codeViewer = React.createRef<HTMLDivElement>();

  octokit: any;

  subscription: Subscription;

  componentWillMount() {
    document.title = `Code | BuildBuddy`;

    this.octokit = new MyOctokit({
      auth: this.props.user.selectedGroup.githubToken,
    });

    this.fetchCode();

    this.subscription = rpcService.events.subscribe({
      next: (name) => name === "refresh" && this.fetchCode(),
    });
  }
  handleWindowResize() {
    this.editor?.layout();
  }

  componentDidMount() {
    window.addEventListener("resize", () => this.handleWindowResize());
    // TODO(siggisim): select default file based on url
    this.editor = monaco.editor.create(this.codeViewer.current, {
      value: ["// Welcome to BuildBuddy Code!", "", "// Click on a file to the left to get start editing."].join("\n"),
      theme: "vs",
    });

    if (this.state.currentFilePath) {
      this.editor.setModel(this.state.fullPathToModelMap.get(this.state.currentFilePath));
    }

    this.editor.onDidChangeModelContent(() => {
      this.handleContentChanged();
    });
  }

  handleContentChanged() {
    if (this.state.originalFileContents.get(this.state.currentFilePath) === this.editor.getValue()) {
      this.state.changes.delete(this.state.currentFilePath);
      this.state.pathToIncludeChanges.delete(this.state.currentFilePath);
    } else if (this.state.currentFilePath) {
      if (!this.state.changes.get(this.state.currentFilePath)) {
        this.state.pathToIncludeChanges.set(this.state.currentFilePath, true);
      }
      this.state.changes.set(this.state.currentFilePath, this.editor.getValue());
    }
    this.setState({ changes: this.state.changes });
    console.log(this.state.changes);

    // TODO(siggisim): store this in cache.
    localStorage.setItem(this.getStateCacheKey(), JSON.stringify(this.state, stateReplacer));
  }

  getStateCacheKey() {
    let repo = this.currentRepo();
    return `${LOCAL_STORAGE_STATE_KEY}/${repo.owner}/${repo.repo}`;
  }

  componentWillUnmount() {
    window.removeEventListener("resize", () => this.handleWindowResize());
    this.subscription?.unsubscribe();
    this.editor?.dispose();
  }

  componentDidUpdate(prevProps: Props) {
    if (
      this.props.hash !== prevProps.hash ||
      this.props.search != prevProps.search ||
      this.props.path != this.props.path
    ) {
      this.fetchCode();
    }
  }

  currentRepo() {
    let groups = this.props.path?.match(/\/code\/(?<owner>.*)\/(?<repo>.*)/)?.groups;
    return groups || {}; 
  }

  fetchCode() {
    const storedState = localStorage.getItem(this.getStateCacheKey()) ? (JSON.parse(localStorage.getItem(this.getStateCacheKey()), stateReviver) as State) : undefined;
    if (storedState) {
      this.setState(storedState);
      return;
    }

    let repo = this.currentRepo();
    if (!repo.owner || !repo.repo) {
      this.handleRepoClicked();
      return;
    }
    this.setState({ owner: repo.owner, repo: repo.repo }, () => {
      this.octokit.request(`/repos/${this.state.owner}/${this.state.repo}/git/trees/master`).then((response: any) => {
        console.log(response);
        this.setState({ repoResponse: response });
      });
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
        this.setState({ treeShaToExpanded: this.state.treeShaToExpanded });
        return;
      }

      this.octokit
        .request(`/repos/${this.state.owner}/${this.state.repo}/git/trees/${node.sha}`)
        .then((response: any) => {
          this.state.treeShaToExpanded.set(node.sha, true);
          this.state.treeShaToChildrenMap.set(node.sha, response.data.tree);
          this.setState({
            treeShaToChildrenMap: this.state.treeShaToChildrenMap,
            treeShaToPathMap: this.state.treeShaToPathMap,
          });
          console.log(response);
        });
      return;
    }

    this.octokit.rest.git
      .getBlob({
        owner: this.state.owner,
        repo: this.state.repo,
        file_sha: node.sha,
      })
      .then((response: any) => {
        console.log(response);
        let fileContents = atob(response.data.content);
        this.state.originalFileContents.set(fullPath, fileContents);
        this.setState({
          currentFilePath: fullPath,
          originalFileContents: this.state.originalFileContents,
          changes: this.state.changes,
        });
        let model = this.state.fullPathToModelMap.get(fullPath);
        if (!model) {
          model = monaco.editor.createModel(fileContents, undefined, monaco.Uri.file(fullPath));
          this.state.fullPathToModelMap.set(fullPath, model);
        }
        this.editor.setModel(model);
      });
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

  handleBuildClicked() {
    let request = new runner.RunRequest();
    request.gitRepo = new runner.RunRequest.GitRepo();
    request.gitRepo.repoUrl = `https://github.com/${this.state.owner}/${this.state.repo}.git`;
    request.bazelCommand = `build //... ${this.getBazelFlags()}`;
    request.repoState = this.getRepoState();

    rpcService.service
      .run(request)
      .then((response: runner.RunResponse) => {
        window.open(`/invocation/${response.invocationId}`, "_blank");
      })
      .catch((error: any) => {
        alert(error);
      });
  }

  handleTestClicked() {
    let request = new runner.RunRequest();
    request.gitRepo = new runner.RunRequest.GitRepo();
    request.gitRepo.repoUrl = `https://github.com/${this.state.owner}/${this.state.repo}.git`;
    request.bazelCommand = `test //... ${this.getBazelFlags()}`;
    request.repoState = this.getRepoState();

    rpcService.service
      .run(request)
      .then((response: runner.RunResponse) => {
        window.open(`/invocation/${response.invocationId}`, "_blank");
      })
      .catch((error: any) => {
        alert(error);
      });
  }

  getRepoState() {
    let state = new runner.RunRequest.RepoState();
    // TODO(siggisim): add commit sha
    for (let path of this.state.changes.keys()) {
      state.patch.push(
        diff.createTwoFilesPatch(
          `a/${path}`,
          `b/${path}`,
          this.state.originalFileContents.get(path),
          this.state.changes.get(path)
        )
      );
    }
    return state;
  }

  async handleReviewClicked() {
    this.setState({ requestingReview: true });

    let filteredEntries = Array.from(this.state.changes.entries()).filter(
      ([key, value]) => this.state.pathToIncludeChanges.get(key) // Only include checked changes
    );

    let filenames = filteredEntries.map(([key, value]) => key).join(", ");

    let response = await this.octokit.createPullRequest({
      owner: this.state.owner,
      repo: this.state.repo,
      title: `Quick fix of ${filenames}`,
      body: `Quick fix of ${filenames} using BuildBuddy Code`,
      head: `quick-fix-${Math.floor(Math.random() * 10000)}`,
      changes: [
        {
          files: Object.fromEntries(
            filteredEntries.map(([key, value]) => [key, { content: btoa(value), encoding: "base64" }]) // Convert to base64 for github to support utf-8
          ),
          commit: `Quick fix of ${this.state.currentFilePath} using BuildBuddy Code`,
        },
      ],
    });

    this.setState({ requestingReview: false });

    window.open(response.data.html_url, "_blank");

    console.log(response);
  }

  handleChangeClicked(fullPath: string) {
    this.setState({ currentFilePath: fullPath });
    this.editor.setModel(this.state.fullPathToModelMap.get(fullPath));
  }

  // TODO(siggisim): Enable users to revert individual changes
  handleCheckboxClicked(fullPath: string) {
    this.state.pathToIncludeChanges.set(fullPath, !this.state.pathToIncludeChanges.get(fullPath));
    this.setState({ pathToIncludeChanges: this.state.pathToIncludeChanges });
  }

  // TODO(siggisim): Implement delete
  handleDeleteClicked(fullPath: string) {}

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
      this.setState(
        {
          currentFilePath: fileName,
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

  handleRepoClicked() {
    let regex = /(?<owner>.*)\/(?<repo>.*)/;
    const groups = prompt("Enter a github repo to edit (i.e. buildbuddy-io/buildbuddy):").match(regex)?.groups;
    if (!groups?.owner || !groups?.repo) {
      alert("Repo not found!");
      this.handleRepoClicked();
    }
    let path = `/code/${groups?.owner}/${groups?.repo}`;
    if (!this.state.owner || !this.state.repo) {
      window.history.pushState({ path: path }, "", path);
    } else {
      window.location.href = path;
    }
  }

  handleGitHubClicked() {
    const params = new URLSearchParams({
      group_id: this.props.user?.selectedGroup?.id,
      redirect_url: window.location.href,
    });
    window.location.href = `/auth/github/link/?${params}`;
  }

  // TODO(siggisim): Make the menu look nice
  // TODO(siggisim): Make sidebar look nice
  // TODO(siggisim): Make the diff view look nicer
  render() {
    setTimeout(() => {
      this.editor?.layout();
    }, 0);

    // TODO(siggisim): Make menu less cluttered
    return (
      <div className="code-editor">
        <div className="code-menu">
          <div className="code-menu-logo">
            <a href="/">
              <img alt="BuildBuddy Code" src="/image/logo_dark.svg" className="logo" /> Code{" "}
              <img src="/image/code.svg" className="code-logo" />
            </a>
          </div>
          <div className="code-menu-actions">
            {!this.props.user.selectedGroup.githubToken && (
              <button onClick={this.handleGitHubClicked.bind(this)}>🔗 &nbsp;Link GitHub</button>
            )}
            <button onClick={this.handleRepoClicked.bind(this)}>👩‍💻 &nbsp;Repo</button>
            <button onClick={this.handleNewFileClicked.bind(this)}>🌱 &nbsp;File</button>
            {/* <button onClick={this.handleDeleteClicked.bind(this)}>❌ &nbsp;Delete</button> */}
            <button onClick={this.handleBuildClicked.bind(this)}>🏗️ &nbsp;Build</button>
            <button onClick={this.handleTestClicked.bind(this)}>🧪 &nbsp;Test</button>
          </div>
        </div>
        <div className="code-main">
          <div className="code-sidebar">
            {this.state.repoResponse &&
              this.state.repoResponse.data.tree.map((node: any) => (
                <SidebarNodeComponent
                  node={node}
                  treeShaToExpanded={this.state.treeShaToExpanded}
                  treeShaToChildrenMap={this.state.treeShaToChildrenMap}
                  handleFileClicked={this.handleFileClicked.bind(this)}
                  fullPath={node.path}
                />
              ))}
          </div>
          <div className="code-container">
            <div className="code-viewer-container">
              <div className="code-viewer" ref={this.codeViewer} />
            </div>
            {this.state.changes.size > 0 && (
              <div className="code-diff-viewer">
                <div className="code-diff-viewer-title">
                  Changes{" "}
                  <button
                    disabled={this.state.requestingReview}
                    className="request-review-button"
                    onClick={this.handleReviewClicked.bind(this)}>
                    {this.state.requestingReview ? "⌛  Requesting..." : "✋  Request Review"}
                  </button>
                </div>
                {Array.from(this.state.changes.keys()).map((fullPath) => (
                  <div className="code-diff-viewer-item" onClick={() => this.handleChangeClicked(fullPath)}>
                    <input
                      checked={this.state.pathToIncludeChanges.get(fullPath)}
                      onChange={(event) => this.handleCheckboxClicked(fullPath)}
                      type="checkbox"
                    />{" "}
                    {fullPath}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
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
      console.log(value.value.map((e: any) => e.uri.path));
      return new Map(value.value.map((e: any) => [e.key, monaco.editor.createModel(e.value, undefined, e.uri.path)]));
    }
  }
  return value;
}
