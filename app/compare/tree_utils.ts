import { TreeNode } from "../invocation/invocation_action_tree_node";

export function findNodeByName(nodes: TreeNode[], name: string): TreeNode | undefined {
  return nodes.find((n) => n.obj.name === name);
}

export function nodesEqual(nodeA?: TreeNode, nodeB?: TreeNode): boolean {
  if (!nodeA && !nodeB) return true;
  if (!nodeA || !nodeB) return false;
  if (nodeA.type !== nodeB.type) return false;

  if (nodeA.type === "file" && nodeB.type === "file") {
    return (
      nodeA.obj.digest?.hash === nodeB.obj.digest?.hash &&
      nodeA.obj.digest?.sizeBytes === nodeB.obj.digest?.sizeBytes &&
      nodeA.obj.isExecutable === nodeB.obj.isExecutable
    );
  }

  if (nodeA.type === "dir" && nodeB.type === "dir") {
    return nodeA.obj.digest?.hash === nodeB.obj.digest?.hash;
  }

  if (nodeA.type === "symlink" && nodeB.type === "symlink") {
    return nodeA.obj.target === nodeB.obj.target;
  }

  return false;
}
