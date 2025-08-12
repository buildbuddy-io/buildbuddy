import React from "react";
import { build } from "../../proto/remote_execution_ts_proto";
import { TreeNode } from "../invocation/invocation_action_tree_node";
import DiffTreeNodeComponent from "./diff_tree_node";
import rpcService from "../service/rpc_service";
import InvocationModel from "../invocation/invocation_model";
import { findNodeByName, nodesEqual, hasChildDifferences } from "./tree_utils";

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

interface TreeState {
  inputRoot?: build.bazel.remote.execution.v2.Directory;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, TreeNode[]>;
}

interface State {
  treeA: TreeState;
  treeB: TreeState;
}

export default class TreeDifferComponent extends React.Component<Props, State> {
  state: State = {
    treeA: this.createTreeState(),
    treeB: this.createTreeState(),
  };

  createTreeState(): TreeState {
    return {
      inputRoot: undefined,
      treeShaToExpanded: new Map<string, boolean>(),
      treeShaToChildrenMap: new Map<string, TreeNode[]>(),
    };
  }

  componentDidMount() {
    this.fetchInputRootsAndExpand();
  }

  componentDidUpdate(prevProps: Props) {
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

  private async fetchInputRootsAndExpand() {
    const [inputRootA, inputRootB] = await Promise.all([
      this.fetchInputRoot(this.props.actionA),
      this.fetchInputRoot(this.props.actionB)
    ]);

    this.setState({
      treeA: { ...this.state.treeA, inputRoot: inputRootA },
      treeB: { ...this.state.treeB, inputRoot: inputRootB },
    }, () => {
      this.expandDifferingNodes();
    });
  }

  private async expandDifferingNodes() {
    const inputNodesA = this.extractInputNodes(this.state.treeA.inputRoot);
    const inputNodesB = this.extractInputNodes(this.state.treeB.inputRoot);

    await this.recursivelyExpandDifferences(
      inputNodesA,
      inputNodesB,
    );

    this.forceUpdate();
  }
  
  private async expandADifferences(nodeA: TreeNode, nodeB?: TreeNode) {
    if (nodeA.type === "symlink") {
      return;
    }
    const digestString = nodeA.obj.digest?.hash || "";
          
    // Expand this directory
    if (digestString) {
      this.state.treeA.treeShaToExpanded.set(digestString, true);
      
      // Fetch children for side A
      const childrenA = await this.fetchDirectoryChildren(nodeA.obj.digest, this.props.actionA) || [];
      if (childrenA.length > 0) {
        this.state.treeA.treeShaToChildrenMap.set(digestString, childrenA);
      }
      
      // If there's a corresponding directory in B, expand it too
      let childrenB: TreeNode[] = [];
      if (nodeB && nodeB.type === "dir") {
        const dirNodeB = nodeB.obj as build.bazel.remote.execution.v2.DirectoryNode;
        const digestStringB = dirNodeB.digest?.hash || "";
        if (digestStringB) {
          this.state.treeB.treeShaToExpanded.set(digestStringB, true);
          
          // Fetch children for side B
          childrenB = await this.fetchDirectoryChildren(dirNodeB.digest, this.props.actionB) || [];
          if (childrenB.length > 0) {
            this.state.treeB.treeShaToChildrenMap.set(digestStringB, childrenB);
          }
        }
      }
      
      // Recursively check children
      if (childrenA.length > 0 || childrenB.length > 0) {
        return await this.recursivelyExpandDifferences(
          childrenA,
          childrenB,
        );
      }
    }
  }

  private async expandBDifferences(nodesA: TreeNode[], nodeB: TreeNode) {
    if (nodeB.type === "dir") {
      const dirNodeB = nodeB.obj as build.bazel.remote.execution.v2.DirectoryNode;
      const nodeA = findNodeByName(nodesA, dirNodeB.name || "");
      
      if (!nodeA) {
        // This directory only exists in B, expand it
        const digestStringB = dirNodeB.digest?.hash || "";
        if (digestStringB) {
          this.state.treeB.treeShaToExpanded.set(digestStringB, true);
          
          // Fetch children for side B
          const childrenB = await this.fetchDirectoryChildren(dirNodeB.digest, this.props.actionB) || [];
          if (childrenB.length > 0) {
            this.state.treeB.treeShaToChildrenMap.set(digestStringB, childrenB);
            
            // Recursively expand all children since this entire subtree is new
            return await this.recursivelyExpandDifferences(
              [],
              childrenB,
            );
          }
        }
      }
    }
  }

  private async expandChildDifferences(nodeA: TreeNode, nodeB: TreeNode) {
    // Even if the directories are equal at this level, check if their contents differ
    const dirNodeA = nodeA.obj as build.bazel.remote.execution.v2.DirectoryNode;
    const dirNodeB = nodeB.obj as build.bazel.remote.execution.v2.DirectoryNode;
    
    // Fetch both directories to check their contents          
    let childrenA = await this.fetchDirectoryChildren(dirNodeA.digest, this.props.actionA) || [];
    let childrenB = await this.fetchDirectoryChildren(dirNodeB.digest, this.props.actionB) || [];
    
    // Check if any child differs
    let shouldExpand = hasChildDifferences(childrenA, childrenB);
    
    if (shouldExpand) {
      // Expand both directories
      const digestStringA = dirNodeA.digest?.hash || "";
      const digestStringB = dirNodeB.digest?.hash || "";
      
      if (digestStringA) {
        this.state.treeA.treeShaToExpanded.set(digestStringA, true);
        this.state.treeA.treeShaToChildrenMap.set(digestStringA, childrenA);
      }
      
      if (digestStringB) {
        this.state.treeB.treeShaToExpanded.set(digestStringB, true);
        this.state.treeB.treeShaToChildrenMap.set(digestStringB, childrenB);
      }
      
      // Recursively expand children
      return await this.recursivelyExpandDifferences(
        childrenA,
        childrenB,
      );
    }
  }

  private async recursivelyExpandDifferences(
    nodesA: TreeNode[],
    nodesB: TreeNode[],
  ): Promise<void> {
    // Check each node in A
    for (const nodeA of nodesA) {
      if (nodeA.type === "dir") {
        const dirNodeA = nodeA.obj as build.bazel.remote.execution.v2.DirectoryNode;
        const nodeB = findNodeByName(nodesB, dirNodeA.name || "");
        
        // Check if this directory differs or doesn't exist in B
        if (!nodesEqual(nodeA, nodeB) || !nodeB) {
          await this.expandADifferences(nodeA, nodeB);
        } else if (nodeB && nodeB.type === "dir") {
          await this.expandChildDifferences(nodeA, nodeB);
        }
      }
    }

    // Check directories that exist only in B
    for (const nodeB of nodesB) {
      // TODO(siggisim): Add a toggle to expand added directories
      // await this.expandBDifferences(nodesA, nodeB);
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

  private handleFileClicked = async (node: TreeNode, side: 'A' | 'B') => {
    if (node.type !== "dir" && node.type !== "tree") return;
    
    const dirNode = node.obj as build.bazel.remote.execution.v2.DirectoryNode;
    const digestString = dirNode.digest?.hash || "";
    
    const treeKey = side === 'A' ? 'treeA' : 'treeB';
    const action = side === 'A' ? this.props.actionA : this.props.actionB;
    const treeState = this.state[treeKey];
    
    const isExpanded = treeState.treeShaToExpanded.get(digestString);
    const newExpanded = new Map(treeState.treeShaToExpanded);
    newExpanded.set(digestString, !isExpanded);
    this.setState({ 
      [treeKey]: { ...treeState, treeShaToExpanded: newExpanded }
    } as any);
    
    // Fetch children if not already fetched
    if (!isExpanded && !treeState.treeShaToChildrenMap.has(digestString) && dirNode.digest && action?.invocationModel) {
      const children = await this.fetchDirectoryChildren(dirNode.digest, action);
      if (children) {
        const newChildrenMap = new Map(treeState.treeShaToChildrenMap);
        newChildrenMap.set(digestString, children);
        this.setState({ 
          [treeKey]: { ...this.state[treeKey], treeShaToChildrenMap: newChildrenMap }
        } as any);
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


  render() {    
    const inputNodesA = this.extractInputNodes(this.state.treeA.inputRoot);
    const inputNodesB = this.extractInputNodes(this.state.treeB.inputRoot);
    
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
                treeState={this.state.treeA}
                otherTreeState={this.state.treeB}
                handleFileClicked={(node: TreeNode) => this.handleFileClicked(node, 'A')}
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
                treeState={this.state.treeB}
                otherTreeState={this.state.treeA}
                handleFileClicked={(node: TreeNode) => this.handleFileClicked(node, 'B')}
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
