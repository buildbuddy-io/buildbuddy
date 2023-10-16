import { cache } from "../../proto/cache_ts_proto";
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
  private durations: Map<number, number>;

  static loading(invocationId: string): ExecutionGraphModel {
    return new ExecutionGraphModel(invocationId, true, false, [], undefined);
  }

  static error(invocationId: string): ExecutionGraphModel {
    return new ExecutionGraphModel(invocationId, false, true, [], undefined);
  }

  static forNodes(
    invocationId: string,
    graphData: execution_graph.Node[],
    scoreCard?: cache.ScoreCard
  ): ExecutionGraphModel {
    return new ExecutionGraphModel(invocationId, false, false, graphData, scoreCard);
  }

  private constructor(
    invocationId: string,
    loading: boolean,
    error: boolean,
    graphData: execution_graph.Node[],
    scoreCard?: cache.ScoreCard
  ) {
    this.invocationId = invocationId;
    this.loading = loading;
    this.error = error;

    this.nodes = new Map();
    this.incomingEdges = new Map();
    this.outgoingEdges = new Map();
    this.targetLabelToNodes = new Map();
    this.durations = new Map();

    // Join up everything..
    console.log("SCORECARD...");
    console.log(scoreCard);

    graphData.forEach((node) => {
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
    });

    console.log("LOGGING..");
    // Compute timing..
    graphData.forEach((node) => {
      const start = +(node.metrics?.startTimestampMillis ?? 0);
      if (!start) {
        return;
      }
      if (+(node.metrics?.durationMillis ?? 0) > 0) {
        this.durations.set(node.index, +start + node.metrics.durationMillis);
        return;
      }

      const possibleEndTime = Array.from(this.outgoingEdges.get(node.index) ?? []).reduce(
        (lowestStartTime, currentNode) => {
          const node = this.getNode(currentNode);
          if (!node) {
            return Math.min(lowestStartTime, Infinity);
          }
          return Math.min(lowestStartTime, +(node.metrics?.startTimestampMillis ?? Infinity));
        },
        Infinity
      );
      if (possibleEndTime < Infinity) {
        this.durations.set(node.index, possibleEndTime);
      }
    });

    console.log("CHECKY TIME...");
    graphData.forEach((node) => {
      const end = this.durations.get(node.index);
      if (!end) {
        console.log("NO END TIME: " + node.index);
        return;
      }
      (this.getOutgoingEdges(node.index) ?? []).forEach((out) => {
        if (+this.getNode(out)!.metrics!.startTimestampMillis < end) {
          console.log("faaaaack");
          console.log(this.getNode(out)!.mnemonic);
          console.log(node);
          console.log(this.getNode(out));
        }
      });
    });
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

  getNode(id: number): Node | undefined {
    return this.nodes.get(id);
  }

  getAllNodes() {
    // Something something immutability something.
    return this.nodes;
  }

  getOutgoingEdges(id: number): number[] {
    return [...(this.outgoingEdges.get(id) ?? [])];
  }

  getIncomingEdges(id: number): number[] {
    return [...(this.incomingEdges.get(id) ?? [])];
  }

  subgraphForTarget(targetLabel: string): ExecutionGraphModel {
    const allowedIds: Set<number> = new Set();
    this.getNodesForTarget(targetLabel).forEach((n) => {
      allowedIds.add(n.index);
      n.dependentIndex.forEach((v) => allowedIds.add(v));
    });

    const filteredNodes = Array.from(allowedIds).map((id) => {
      const nodeCopy = execution_graph.Node.create(this.getNode(id));
      if (nodeCopy.index === undefined) {
        // XXX: Shitty.
      }
      nodeCopy.dependentIndex = nodeCopy.dependentIndex.filter((v) => allowedIds.has(v));
      return nodeCopy;
    });

    return ExecutionGraphModel.forNodes(this.invocationId, filteredNodes);
  }

  /**
   * Filters all nodes in this execution graph and finds all nodes that don't
   * depend on any other nodes.  This can be used to find "surprising" targets
   * that were rebuilt even though
   */
  getLeafTargets(): string[] {
    const out: string[] = [];

    for (let [target, nodes] of this.targetLabelToNodes.entries()) {
      // Trying to find even one dep that counts as "real".
      // XXX: Slow.
      // Want things that:
      // 1) are not tools AND
      // 2) dependencies are *all* either:
      //   2a) cached, or
      //   2b) a tool.
      // therefore, looking for a dep that is !cached && !tool (!(cached || tool)
      const great = Array.from(nodes).find((node) => {
        if ((this.getNode(node)?.description.indexOf("[for tool]") ?? -1) >= 0) {
          console.log("SKIP: " + this.getNode(node)?.targetLabel);
          return false;
        }
        const incoming = this.getIncomingEdges(node);
        return incoming.find((n) => {
          const incomingNode = this.getNode(n);
          if (!incomingNode) {
            return false;
          }
          if (incomingNode.runner !== "remote cache hit" && incomingNode.description.indexOf("[for tool]") < 0) {
            console.log(incomingNode.description);
            console.log(incomingNode.runner);
            console.log(incomingNode.runner !== "remote cache hit");
            console.log(incomingNode.description.indexOf("[for tool]"));
            console.log(
              `EHHH: ${
                incomingNode.runner !== "remote cache hit" && incomingNode.description.indexOf("[for tool]") < 0
              }`
            );
          }
          return incomingNode.runner !== "remote cache hit" && incomingNode.description.indexOf("[for tool]") < 0;
        });
      });
      if (great) {
        out.push(target);
      }
    }
    return out;
  }

  weirdConstrainedSubgraphForTarget(targetLabel: string): ExecutionGraphModel {
    const labelIds: Set<number> = new Set();
    const allowedIds: Set<number> = new Set();
    const depsOfOthers: Set<number> = new Set();
    this.getNodesForTarget(targetLabel).forEach((n) => {
      labelIds.add(n.index);
      allowedIds.add(n.index);
    });
    this.getNodesForTarget(targetLabel).forEach((n) => {
      n.dependentIndex.forEach((v) => {
        if (!labelIds.has(v)) {
          const dep = this.getIncomingEdges(v);
          dep?.forEach((innerDep) => depsOfOthers.add(innerDep));
          allowedIds.add(v);
        }
      });
    });

    const filteredNodes = Array.from(allowedIds).map((id) => {
      const nodeCopy = execution_graph.Node.create(this.getNode(id));
      if (nodeCopy.index === undefined) {
        // XXX Shitty
      }
      // XXX Shitty.
      nodeCopy.dependentIndex = nodeCopy.dependentIndex.filter(
        (v) => labelIds.has(nodeCopy.index) && allowedIds.has(v) && !depsOfOthers.has(v)
      );
      return nodeCopy;
    });

    return ExecutionGraphModel.forNodes(this.invocationId, filteredNodes);
  }

  constrainedSubgraphForTarget(targetLabel: string): ExecutionGraphModel {
    const labelIds: Set<number> = new Set();
    const allowedIds: Set<number> = new Set();
    this.getNodesForTarget(targetLabel).forEach((n) => {
      labelIds.add(n.index);
      allowedIds.add(n.index);
      n.dependentIndex.forEach((v) => allowedIds.add(v));
    });

    const filteredNodes = Array.from(allowedIds).map((id) => {
      const nodeCopy = execution_graph.Node.create(this.getNode(id));
      if (nodeCopy.index === undefined) {
        // XXX Shitty
      }
      // XXX Shitty.
      nodeCopy.dependentIndex = nodeCopy.dependentIndex.filter(
        (v) => labelIds.has(nodeCopy.index) && allowedIds.has(v)
      );
      return nodeCopy;
    });

    return ExecutionGraphModel.forNodes(this.invocationId, filteredNodes);
  }
}
