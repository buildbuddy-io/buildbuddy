import { execution_graph } from "../../proto/execution_graph_ts_proto";

type Node = execution_graph.Node;

export default class ExecutionGraphModel {
  private invocationId: string;
  private loading: boolean;
  private error: boolean;

  private nodes: Map<number, Node>;
  private incomingEdges: Map<number, Set<number>>;
  private outgoingEdges: Map<number, Set<number>>;
  private targetLabelToNodes: Map<string, Set<number>>;

  static loading(invocationId: string): ExecutionGraphModel {
    return new ExecutionGraphModel(invocationId, true, false, []);
  }

  static error(invocationId: string): ExecutionGraphModel {
    return new ExecutionGraphModel(invocationId, false, true, []);
  }

  static forNodes(invocationId: string, graphData: execution_graph.Node[]): ExecutionGraphModel {
    return new ExecutionGraphModel(invocationId, false, false, graphData);
  }

  private constructor(invocationId: string, loading: boolean, error: boolean, graphData: execution_graph.Node[]) {
    this.invocationId = invocationId;
    this.loading = loading;
    this.error = error;

    this.nodes = new Map();
    this.incomingEdges = new Map();
    this.outgoingEdges = new Map();
    this.targetLabelToNodes = new Map();
    for (let i = 0; i < graphData.length; i++) {
      const node = graphData[i];
      this.nodes.set(node.index, node);

      let labelSet = this.targetLabelToNodes.get(node.targetLabel);
      if (!labelSet) {
        labelSet = new Set();
        this.targetLabelToNodes.set(node.targetLabel, labelSet);
      }
      labelSet.add(node.index);

      let incoming = this.incomingEdges.get(node.index);
      if (incoming === undefined) {
        incoming = new Set();
        this.incomingEdges.set(node.index, incoming);
      }

      for (let j = 0; j < node.dependentIndex.length; j++) {
        const depIndex = node.dependentIndex[j];
        let outgoing = this.outgoingEdges.get(depIndex);
        if (outgoing === undefined) {
          outgoing = new Set();
          this.outgoingEdges.set(depIndex, outgoing);
        }

        incoming.add(depIndex);
        outgoing.add(node.index);
      }
    }
  }

  getInvocationId(): string {
    return this.invocationId;
  }

  isLoading(): boolean {
    return this.loading;
  }

  isError(): boolean {
    return this.error;
  }

  getNodesForTarget(target: string): Node[] {
    const nodeSet = this.targetLabelToNodes.get(target);
    if (!nodeSet) {
      return [];
    }
    const out: Node[] = [];
    nodeSet.forEach((id) => {
      const node = this.nodes.get(id);
      if (node) {
        out.push(node);
      }
    });
    return out;
  }
}
