import React from "react";
import { build } from "../../proto/remote_execution_ts_proto";
import { TreeNode } from "../invocation/invocation_action_tree_node";
import InvocationModel from "../invocation/invocation_model";
import rpcService from "../service/rpc_service";
import DiffTreeNodeComponent from "./diff_tree_node";
import { findNodeByName, nodesEqual } from "./tree_utils";

interface Props {
  actionA?: ActionDetails;
  actionB?: ActionDetails;
  showChangesOnly: boolean;
}

interface ActionDetails {
  invocationId: string;
  invocationModel?: InvocationModel;
  action?: build.bazel.remote.execution.v2.Action;
}

interface State {
  treeA?: build.bazel.remote.execution.v2.Directory;
  treeB?: build.bazel.remote.execution.v2.Directory;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, TreeNode[]>;
}

export default class TreeDifferComponent extends React.Component<Props, State> {
  state: State = {
    treeA: undefined,
    treeB: undefined,
    treeShaToExpanded: new Map<string, boolean>(),
    treeShaToChildrenMap: new Map<string, TreeNode[]>(),
  };

  componentDidMount(): void {
    this.fetchInputRootsAndExpand();
  }

  componentDidUpdate(prevProps: Props): void {
    if (prevProps.actionA !== this.props.actionA || prevProps.actionB !== this.props.actionB) {
      this.fetchInputRootsAndExpand();
    }
  }

  private async fetchInputRoot(action?: ActionDetails): Promise<build.bazel.remote.execution.v2.Directory | undefined> {
    if (!action?.action?.inputRootDigest || !action?.invocationModel) {
      return undefined;
    }
    const inputRootUrl = action?.invocationModel?.getBytestreamURL(action?.action?.inputRootDigest);
    const buffer = await rpcService.fetchBytestreamFile(inputRootUrl, action.invocationId, "arraybuffer");
    return build.bazel.remote.execution.v2.Directory.decode(new Uint8Array(buffer));
  }

  private async fetchInputRootsAndExpand(): Promise<void> {
    const [inputRootA, inputRootB] = await Promise.all([
      this.fetchInputRoot(this.props.actionA),
      this.fetchInputRoot(this.props.actionB),
    ]);

    this.setState(
      {
        treeA: inputRootA,
        treeB: inputRootB,
      },
      () => {
        this.expandDifferingNodes();
      }
    );
  }

  private async expandDifferingNodes(): Promise<void> {
    const inputNodesA = this.extractInputNodes(this.state.treeA);
    const inputNodesB = this.extractInputNodes(this.state.treeB);

    await this.recursivelyExpandDifferences(inputNodesA, inputNodesB);

    this.forceUpdate();
  }

  private async expandDifferences(nodeA: TreeNode, nodeB?: TreeNode): Promise<void> {
    let childrenA: TreeNode[] = [];
    if (nodeA.type == "dir") {
      this.state.treeShaToExpanded.set(nodeA.obj.digest?.hash || "", true);
      childrenA = (await this.fetchDirectoryChildren(nodeA.obj.digest, this.props.actionA)) || [];
      if (childrenA.length > 0) {
        this.state.treeShaToChildrenMap.set(nodeA.obj.digest?.hash || "", childrenA);
      }
    }

    let childrenB: TreeNode[] = [];
    if (nodeB && nodeB.type === "dir") {
      this.state.treeShaToExpanded.set(nodeB.obj.digest?.hash || "", true);

      // Fetch children for side B
      childrenB = (await this.fetchDirectoryChildren(nodeB.obj.digest, this.props.actionB)) || [];
      if (childrenB.length > 0) {
        this.state.treeShaToChildrenMap.set(nodeB.obj.digest?.hash || "", childrenB);
      }
    }

    // Recursively check children
    if (childrenA.length > 0 || childrenB.length > 0) {
      return await this.recursivelyExpandDifferences(childrenA, childrenB);
    }
  }

  private async recursivelyExpandDifferences(nodesA: TreeNode[], nodesB: TreeNode[]): Promise<void> {
    // Check each node in A
    for (const nodeA of nodesA) {
      if (nodeA.type === "dir") {
        const dirNodeA = nodeA.obj as build.bazel.remote.execution.v2.DirectoryNode;
        const nodeB = findNodeByName(nodesB, dirNodeA.name || "");

        // Check if this directory differs or doesn't exist in B
        if (!nodeB) {
          // Don't expanded deleted directories.
        } else if (!nodesEqual(nodeA, nodeB)) {
          await this.expandDifferences(nodeA, nodeB);
        }
      }
    }
  }

  private extractInputNodes(inputRoot?: build.bazel.remote.execution.v2.Directory): TreeNode[] {
    if (!inputRoot) return [];

    const nodes: TreeNode[] = [];

    // Add files
    inputRoot.files?.forEach((file: build.bazel.remote.execution.v2.FileNode) => {
      nodes.push({ type: "file", obj: file });
    });

    // Add directories
    inputRoot.directories?.forEach((dir: build.bazel.remote.execution.v2.DirectoryNode) => {
      nodes.push({ type: "dir", obj: dir });
    });

    // Add symlinks
    inputRoot.symlinks?.forEach((symlink: build.bazel.remote.execution.v2.SymlinkNode) => {
      nodes.push({ type: "symlink", obj: symlink });
    });

    // Sort by name
    nodes.sort((a, b) => {
      const nameA = a.obj.name || "";
      const nameB = b.obj.name || "";
      return nameA.localeCompare(nameB);
    });

    return nodes;
  }

  private handleDirectoryClicked = async (node: TreeNode, side: "A" | "B"): Promise<void> => {
    if (node.type !== "dir") return;

    const dirNode = node.obj as build.bazel.remote.execution.v2.DirectoryNode;
    const digestString = dirNode.digest?.hash || "";

    const action = side === "A" ? this.props.actionA : this.props.actionB;

    const isExpanded = this.state.treeShaToExpanded.get(digestString);
    const newExpanded = new Map(this.state.treeShaToExpanded);
    newExpanded.set(digestString, !isExpanded);
    this.setState({ treeShaToExpanded: newExpanded });

    // Fetch children if not already fetched
    if (
      !isExpanded &&
      !this.state.treeShaToChildrenMap.has(digestString) &&
      dirNode.digest &&
      action?.invocationModel
    ) {
      const children = await this.fetchDirectoryChildren(dirNode.digest, action);
      if (children) {
        const newChildrenMap = new Map(this.state.treeShaToChildrenMap);
        newChildrenMap.set(digestString, children);
        this.setState({ treeShaToChildrenMap: newChildrenMap });
      }
    }
  };

  private async fetchDirectoryChildren(
    digest: build.bazel.remote.execution.v2.Digest | null | undefined,
    action?: ActionDetails
  ): Promise<TreeNode[] | undefined> {
    if (!digest || !action?.invocationModel) {
      return undefined;
    }

    const dirUrl = action.invocationModel.getBytestreamURL(digest);
    const buffer = await rpcService.fetchBytestreamFile(dirUrl, action.invocationId, "arraybuffer");
    const dir = build.bazel.remote.execution.v2.Directory.decode(new Uint8Array(buffer));
    return this.extractInputNodes(dir);
  }

  render(): JSX.Element {
    const inputNodesA = this.extractInputNodes(this.state.treeA);
    const inputNodesB = this.extractInputNodes(this.state.treeB);

    if (!inputNodesA.length && !inputNodesB.length) {
      return <div className="no-input-files">No input files found</div>;
    }

    return (
      <div className="tree-differ-container">
        <div className="tree-differ-header">
          <div className="tree-column-header">Action A</div>
          <div className="tree-column-header">Action B</div>
        </div>
        <div className="tree-differ-content">
          <div className="compare-tree-columns">
            <div className="tree-column">
              <DiffTreeNodeComponent
                nodes={inputNodesA}
                otherNodes={inputNodesB}
                side="left"
                treeA={this.state.treeA}
                treeB={this.state.treeB}
                treeShaToExpanded={this.state.treeShaToExpanded}
                treeShaToChildrenMap={this.state.treeShaToChildrenMap}
                handleDirectoryClicked={(node: TreeNode) => this.handleDirectoryClicked(node, "A")}
                showChangesOnly={this.props.showChangesOnly}
                actionDetails={this.props.actionA}
                otherActionDetails={this.props.actionB}
              />
            </div>
            <div className="tree-column">
              <DiffTreeNodeComponent
                nodes={inputNodesB}
                otherNodes={inputNodesA}
                side="right"
                treeA={this.state.treeA}
                treeB={this.state.treeB}
                treeShaToExpanded={this.state.treeShaToExpanded}
                treeShaToChildrenMap={this.state.treeShaToChildrenMap}
                handleDirectoryClicked={(node: TreeNode) => this.handleDirectoryClicked(node, "B")}
                showChangesOnly={this.props.showChangesOnly}
                actionDetails={this.props.actionB}
                otherActionDetails={this.props.actionA}
              />
            </div>
          </div>
        </div>
      </div>
    );
  }
}
