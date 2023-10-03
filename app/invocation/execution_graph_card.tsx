import React from "react";
import ExecutionGraphModel from "./execution_graph_model";
import { execution_graph } from "../../proto/execution_graph_ts_proto";

interface Props {
  graph: ExecutionGraphModel;
  selectedTarget?: string;
  selectedIndex?: number;
}

type Options = {
  c?: number;
  bigc?: number;
};

type Link = {
  source: number;
  target: number;
  bundle?: Bundle;
  xt: number;
  yt: number;
  xb: number;
  yb: number;
  c1: number;
  c2: number;
  xs: number;
  ys: number;
};

type BundleArrayWithIndex = {
  bundles: Bundle[];
  bullshitIndex: number;
};

type Bundle = {
  id: string;
  indexInLevel: number;
  level: number;
  span: number;
  parentEntries: number[];
  links: Link[];
  x: number;
  y: number;
};

type Entry = {
  id: number;
  parents: number[];
  height: number;
  x: number;
  y: number;
  level: number;
};

type Level = {
  entries: Entry[];
};

interface State {}

function d3Min(values, valueof) {
  let min;
  if (valueof === undefined) {
    for (const value of values) {
      if (value != null && (min > value || (min === undefined && value >= value))) {
        min = value;
      }
    }
  } else {
    let index = -1;
    for (let value of values) {
      if ((value = valueof(value, ++index, values)) != null && (min > value || (min === undefined && value >= value))) {
        min = value;
      }
    }
  }
  return min;
}

function d3Max(values, valueof) {
  let max;
  if (valueof === undefined) {
    for (const value of values) {
      if (value != null && (max < value || (max === undefined && value >= value))) {
        max = value;
      }
    }
  } else {
    let index = -1;
    for (let value of values) {
      if ((value = valueof(value, ++index, values)) != null && (max < value || (max === undefined && value >= value))) {
        max = value;
      }
    }
  }
  return max;
}

function d3Descending(a: number | null, b: number | null) {
  return a == null || b == null ? NaN : b < a ? -1 : b > a ? 1 : b >= a ? 0 : NaN;
}

function computeDepths(model: ExecutionGraphModel) {
  const depths = new Map<number, number>();
  model.getAllNodes().forEach((node) => {
    longestPathHelper(node.index, depths, model);
  });
  // Can compute max depth using results of above calls.
  return depths;
}

function longestPathHelper(id: number, state: Map<number, number>, model: ExecutionGraphModel): number {
  const value = state.get(id);
  if (value !== undefined) {
    return value;
  }
  const children = model.getOutgoingEdges(id);
  if (children.length === 0) {
    state.set(id, 0);
    return 0;
  }

  const longestPathThroughChildren = children.reduce((longest, childId) => {
    const subgraphLongestPath = longestPathHelper(childId, state, model);
    return 1 + Math.max(subgraphLongestPath, longest);
  });

  state.set(id, longestPathThroughChildren);
  return longestPathThroughChildren;
}

export default class ExecutionGraphCard extends React.Component<Props, State> {
  state: State = {};

  // DATA: {id: number, parents: number[]}[], each array entry is a "depth", first entry is "root".
  renderChart(data: Level[], options: Options): JSX.Element {
    // XXX: options.color ||= (d, i) => color(i);

    const tangleLayout = this.constructTangleLayout(data, options);

    return (
      <svg width={tangleLayout.layout.width} height={tangleLayout.layout.height} style={{ backgroundColor: "white" }}>
        <style>
          {`
          text {
            font-family: sans-serif;
            font-size: 10px;
          }
          .node {
            stroke-linecap: round;
          }
          .link {
            fill: none;
          }`}
        </style>
        {tangleLayout.bundles.map((b, i) => {
          let d = b.links
            .map(
              (l) => `
        M${l.xt} ${l.yt}
        L${l.xb - l.c1} ${l.yt}
        A${l.c1} ${l.c1} 90 0 1 ${l.xb} ${l.yt + l.c1}
        L${l.xb} ${l.ys - l.c2}
        A${l.c2} ${l.c2} 90 0 0 ${l.xb + l.c2} ${l.ys}
        L${l.xs} ${l.ys}`
            )
            .join("");
          return (
            <>
              <path className="link" d={d} stroke="white" stroke-width="5" />
              <path className="link" d={d} stroke="black" stroke-width="2" />
            </>
          );
        })}

        {tangleLayout.nodes.map((n) => {
          return (
            <>
              <path
                className="selectable node"
                data-id={n.id}
                stroke="black"
                stroke-width="8"
                d={`M${n.x} ${n.y - n.height / 2} L${n.x} ${n.y + n.height / 2}`}
              />
              <path
                className="node"
                stroke="white"
                stroke-width="4"
                d={`M${n.x} ${n.y - n.height / 2} L${n.x} ${n.y + n.height / 2}`}
              />
              <text
                className="selectable"
                data-id={n.id}
                x={n.x + 4}
                y={n.y - n.height / 2 - 4}
                stroke="${background_color}"
                stroke-width="2">
                ${n.id}
              </text>
              <text x={n.x + 4} y={n.y - n.height / 2 - 4} style={{ pointerEvents: "none" }}>
                ${n.id}
              </text>
            </>
          );
        })}
      </svg>
    );
  }

  constructTangleLayout(levels: Level[], options: Options) {
    // Okay, this is an absolute mess.  Let's try to pick this apart and
    // construct a bunch of maps instead of continuing to set properties.

    // "Bundles" are collections of nodes with the same parents.
    const bidToBundleMap: Map<string, Bundle> = new Map();

    // Save away "level" for each node.
    const nodeToLevelMap: Map<number, number> = new Map();
    levels.forEach((l, i) => l.entries.forEach((e) => nodeToLevelMap.set(e.id, i)));

    // Make a single list of all entries.
    const entries = levels.reduce((a: Entry[], x) => a.concat(x.entries), []);

    // And a convenience lookup map, sure why not.
    const eidToEntryMap: Map<number, Entry> = new Map();
    entries.forEach((e) => eidToEntryMap.set(e.id, e));

    const eidToParentsMap: Map<number, Entry[]> = new Map();

    entries.forEach((e) => {
      const parents: Entry[] = [];
      eidToParentsMap.set(e.id, parents);
      (e.parents === undefined ? [] : e.parents).forEach((p) => {
        const parent = eidToEntryMap.get(p);
        if (parent) {
          parents.push(parent);
        }
      });
    });

    const eidToSingleBundleMap: Map<number, Bundle> = new Map();
    const levelToBundlesMap: Map<number, Bundle[]> = new Map();
    // Precompute bundles. Bundles are... I don't know what.
    levels.forEach((l, i) => {
      const bidToBundleMap: Map<string, Bundle> = new Map();
      l.entries.forEach((e) => {
        e.level = i;
        if (e.parents.length == 0) {
          return;
        }

        const bid = e.parents.sort((a, b) => a - b).join("-X-");
        let bundle = bidToBundleMap.get(bid);
        if (bundle) {
          // XXX: What the fresh hell is this?
          bundle.parentEntries = bundle.parentEntries.concat(e.parents);
        } else {
          bundle = {
            id: bid,
            indexInLevel: 0 /* set later, i hate this */,
            parentEntries: e.parents.slice(),
            level: i,
            links: [],
            span: i - d3Min(e.parents, (p: any) => nodeToLevelMap.get(p) ?? 0),
            x: 0,
            y: 0,
          };
          bidToBundleMap.set(bid, bundle);
        }
        eidToSingleBundleMap.set(e.id, bundle);
      });

      const bundleList = [];
      for (let [_, value] of bidToBundleMap) {
        bundleList.push(value);
      }
      bundleList.forEach((b, bundleIndex) => (b.indexInLevel = bundleIndex));
      levelToBundlesMap.set(i, bundleList);
    });

    const links: Link[] = [];
    entries.forEach((e) => {
      e.parents.forEach((p) =>
        links.push({
          source: e.id,
          bundle: eidToSingleBundleMap.get(e.id),
          target: p,
          xt: 0,
          yt: 0,
          xb: 0,
          yb: 0,
          c1: 0,
          c2: 0,
          xs: 0,
          ys: 0,
        })
      );
    });

    const bundles = Array.from(levelToBundlesMap).reduce((a: Bundle[], x) => a.concat(x[1]), []);

    // reverse pointer from parent to bundles
    const eidToBidToBundleMap: Map<number, Map<string, Bundle[]>> = new Map();
    bundles.forEach((b) =>
      b.parentEntries.forEach((p) => {
        let bidToBundleMap = eidToBidToBundleMap.get(p);
        if (bidToBundleMap === undefined) {
          bidToBundleMap = new Map();
          eidToBidToBundleMap.set(p, bidToBundleMap);
        }
        let bundleArray = bidToBundleMap.get(b.id);
        if (!bundleArray) {
          bundleArray = [];
          bidToBundleMap.set(b.id, bundleArray);
        }
        bundleArray.push(b);
      })
    );

    // XXX: You are here.
    const eidToBundleSetMap: Map<number, BundleArrayWithIndex[]> = new Map();
    entries.forEach((e) => {
      let bundleSet: BundleArrayWithIndex[] = [];
      const bidToBundleMap = eidToBidToBundleMap.get(e.id);
      if (bidToBundleMap !== undefined) {
        bundleSet = Array.from(bidToBundleMap.values()).map((v) => {
          return { bundles: v, bullshitIndex: 0 };
        });
      }

      // sort bundles by descending span, so that we know what order they need to be drawn in.
      // XXX: did i get this right it was late
      bundleSet.sort((a: BundleArrayWithIndex, b: BundleArrayWithIndex) =>
        d3Descending(
          d3Max(a.bundles, (d: Bundle) => d.span),
          d3Max(b.bundles, (d: Bundle) => d.span)
        )
      );
      // Number off the order that the bundles wound up in.
      bundleSet.forEach((b, i) => (b.bullshitIndex = i));
      eidToBundleSetMap.set(e.id, bundleSet);
    });

    links.forEach((l) => {
      if (!l.bundle) {
        return;
      }
      l.bundle.links.push(l);
    });

    // layout
    const padding = 8;
    const node_height = 22;
    const node_width = 70;
    const bundle_width = 14;
    const level_y_padding = 16;
    const metro_d = 4;
    const min_family_height = 22;

    options.c ||= 16;
    const c = options.c;
    options.bigc ||= node_width + c;

    entries.forEach((e) => {
      const numberOfBundles = eidToBidToBundleMap.get(e.id)?.entries.length ?? 0;
      e.height = (Math.max(1, numberOfBundles) - 1) * metro_d;
    });

    var x_offset = padding;
    var y_offset = padding;
    levels.forEach((l, levelIndex) => {
      const levelBundlesLength = levelToBundlesMap.get(levelIndex)?.length ?? 0;
      x_offset += levelBundlesLength * bundle_width;
      y_offset += level_y_padding;
      l.entries.forEach((n) => {
        n.x = levelIndex * node_width + x_offset;
        n.y = node_height + y_offset + n.height / 2;

        y_offset += node_height + n.height;
      });
    });

    let i = 0;
    levels.forEach((l, levelIndex) => {
      levelToBundlesMap.get(levelIndex)?.forEach((b, bundleIndex, bundleValues) => {
        console.log(b);
        console.log(
          d3Max(b.parentEntries, (d: number) => {
            console.log("d");
            console.log(d);
            console.log(eidToEntryMap.get(d));
            return eidToEntryMap.get(d).x;
          })
        );
        b.x =
          d3Max(b.parentEntries, (d: number) => {
            return eidToEntryMap.get(d).x;
          }) +
          node_width +
          (bundleValues.length - 1 - b.indexInLevel) * bundle_width;
        b.y = i * node_height;
      });
      i += l.entries.length;
    });

    links.forEach((l) => {
      const target = eidToEntryMap.get(l.target);
      const source = eidToEntryMap.get(l.source);
      if (!l.bundle || !target || !source) {
        return;
      }
      const bundleArrays = eidToBundleSetMap.get(target.id);
      if (bundleArrays === undefined) {
        return;
      }
      const bundleArrayWithIndex = bundleArrays.find((b: BundleArrayWithIndex) =>
        b.bundles.find((ff) => ff.id === l.bundle!.id) ? true : false
      );
      if (!bundleArrayWithIndex) {
        return;
      }
      const individualBundle = bundleArrayWithIndex.bundles.find((ff) => ff.id === l.bundle!.id);
      if (!individualBundle) {
        return;
      }

      l.xt = target.x;
      l.yt =
        target.y +
        individualBundle.indexInLevel * metro_d -
        (bundleArrayWithIndex.bundles.length * metro_d) / 2 +
        metro_d / 2;
      l.xb = l.bundle?.x ?? 0;
      l.yb = l.bundle?.y ?? 0;
      l.xs = source.x;
      l.ys = source.y;

      console.log(l);
    });

    // compress vertical space
    var y_negative_offset = 0;
    levels.forEach((l, i) => {
      y_negative_offset +=
        -min_family_height +
          d3Min(levelToBundlesMap.get(i) ?? [], (b: Bundle) =>
            d3Min(b.links, (link: Link) => link.ys - 2 * c - (link.yt + c))
          ) || 0;
      l.entries.forEach((e) => (e.y -= y_negative_offset));
    });

    // very ugly, I know
    links.forEach((l) => {
      const target = eidToEntryMap.get(l.target);
      const source = eidToEntryMap.get(l.source);

      if (!l.bundle || !target || !source) {
        return;
      }

      const bundleArrays = eidToBundleSetMap.get(target.id);
      if (bundleArrays === undefined) {
        return;
      }
      const bundleArrayWithIndex = bundleArrays.find((b: BundleArrayWithIndex) =>
        b.bundles.find((ff) => ff.id === l.bundle!.id) ? true : false
      );
      if (!bundleArrayWithIndex) {
        return;
      }
      const individualBundle = bundleArrayWithIndex.bundles.find((ff) => ff.id === l.bundle!.id);
      if (!individualBundle) {
        return;
      }

      l.yt =
        target.y +
        individualBundle.indexInLevel * metro_d -
        (bundleArrayWithIndex.bundles.length * metro_d) / 2 +
        metro_d / 2;
      l.ys = source.y;
      l.c1 = source.level - target.level > 1 ? Math.min(options.bigc ?? 0, l.xb - l.xt, l.yb - l.yt) - c : c;
      l.c2 = c;
    });

    var layout = {
      width: d3Max(entries, (e: Entry) => e.x) + node_width + 2 * padding,
      height: d3Max(entries, (e: Entry) => e.y) + node_height / 2 + 2 * padding,
      node_height,
      node_width,
      bundle_width,
      level_y_padding,
      metro_d,
    };

    return { levels, nodes: entries, nodes_index: eidToEntryMap, links, bundles, layout };
  }
  /*
  data = [
  [{ id: 'Chaos' }],
  [{ id: 'Gaea', parents: ['Chaos'] }, { id: 'Uranus' }],
]
  */

  render() {
    /*let primaryNodes: execution_graph.Node[];
    if (this.props.selectedIndex !== undefined) {
      const node = this.props.graph.getNode(this.props.selectedIndex);
      primaryNodes = node ? this.props.graph.getNodesForTarget(node.targetLabel) : [];
    } else if (this.props.selectedTarget !== undefined) {
      primaryNodes = this.props.graph.getNodesForTarget(this.props.selectedTarget);
    }*/
    const data = [
      { entries: [this.createEntry(0, [])] },
      { entries: [this.createEntry(1, [0]), this.createEntry(2, [])] },
      { entries: [this.createEntry(3, [0, 1]), this.createEntry(4, [1, 2]), this.createEntry(5, [1])] },
    ];
    return this.renderChart(data, {});
  }

  createEntry(id: number, parents: number[]): Entry {
    return { id, parents, x: 0, y: 0, level: 0, height: 0 };
  }
}
