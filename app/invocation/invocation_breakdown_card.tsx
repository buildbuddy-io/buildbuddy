import React from "react";
import { PieChart as PieChartIcon } from "lucide-react";
import { ResponsiveContainer, PieChart, Pie, Cell } from "recharts";
import format from "../format/format";
import { getChartColor } from "../util/color";

interface Props {
  // The total duration by event names
  durationByNameMap: Map<string, number>;
  // The total duration by event categories
  durationByCategoryMap: Map<string, number>;
}

interface Datum {
  name: string;
  value: number;
}

export default class InvocationBreakdownCardComponent extends React.Component<Props> {
  render() {
    let launching = this.props.durationByNameMap.get("Launch Blaze");
    let total = this.props.durationByNameMap.get("buildTargets");
    let targets = this.props.durationByNameMap.get("evaluateTargetPatterns");
    let analysis = this.props.durationByNameMap.get("runAnalysisPhase");
    let building = total - analysis - targets;

    let runningProcess = this.props.durationByNameMap.get("subprocess.run");
    let localActionExecution = this.props.durationByCategoryMap.get("local action execution");
    let localExecution = runningProcess + localActionExecution;

    let executingRemotely = this.props.durationByNameMap.get("execute remotely");
    let sandboxSetup = this.props.durationByNameMap.get("sandbox.createFileSystem");
    let sandboxTeardown = this.props.durationByNameMap.get("sandbox.delete");
    let inputMapping = this.props.durationByNameMap.get("AbstractSpawnStrategy.getInputMapping");
    let merkleTree = this.props.durationByNameMap.get("MerkleTree.build(ActionInput)");
    let downloadOuputs = this.props.durationByCategoryMap.get("remote output download");
    let uploadMissing = this.props.durationByNameMap.get("upload missing inputs");
    let uploadOutputs = this.props.durationByNameMap.get("upload outputs");
    let checkCache = this.props.durationByNameMap.get("check cache hit");
    let detectModifiedOutput = this.props.durationByNameMap.get("detectModifiedOutputFiles");
    let stableStatus = this.props.durationByNameMap.get("BazelWorkspaceStatusAction stable-status.txt");

    let phaseData = [
      { value: launching, name: "Launch" },
      { value: targets, name: "Evaluation" },
      { value: analysis, name: "Analysis" },
      { value: building, name: "Execution" },
    ];

    phaseData = phaseData.sort((a, b) => b.value - a.value).filter((entry) => entry.value > 0);

    let executionData = [
      { value: localExecution, name: "Executing locally" },
      { value: inputMapping, name: "Input mapping" },
      { value: merkleTree, name: "Merkle tree building" },
      { value: sandboxSetup, name: "Local sandbox creation" },
      { value: sandboxTeardown, name: "Local sandbox teardown" },
      { value: executingRemotely, name: "Executing remotely" },
      { value: checkCache, name: "Checking cache hits" },
      { value: uploadMissing, name: "Uploading missing inputs" },
      { value: downloadOuputs, name: "Downloading outputs" },
      { value: uploadOutputs, name: "Uploading outputs" },
      { value: detectModifiedOutput, name: "Detect modified output files" },
      { value: stableStatus, name: "Generating stable-status.txt" },
    ];

    executionData = executionData.sort((a, b) => (b?.value || 0) - (a?.value || 0)).filter((entry) => entry.value > 0);

    return (
      <div className="card">
        <PieChartIcon className="icon" />
        <div className="content">
          <div className="title">Timing Breakdown</div>
          <div className="details">
            <div className="cache-sections">
              {phaseData.length > 0 && renderBreakdown(phaseData, "Phase breakdown", "Breakdown of build phases")}
              {executionData.length > 0 &&
                renderBreakdown(executionData, "Execution breakdown", "Breakdown totals across all threads")}
            </div>
          </div>
        </div>
      </div>
    );
  }
}

function renderBreakdown(data: Datum[], title: string, subtitle: string) {
  let sum = data.reduce((prev, current) => {
    return { name: "Sum", value: prev.value + current.value };
  });

  return (
    <div className="cache-section">
      <div className="cache-title">{title}</div>
      <div className="cache-subtitle">{subtitle}</div>
      <div className="cache-chart">
        <ResponsiveContainer width={100} height={100}>
          <PieChart>
            <Pie data={data} dataKey="value" outerRadius={40} innerRadius={20}>
              {data.map((_, index) => (
                <Cell key={`cell-${index}`} fill={getChartColor(index)} />
              ))}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
        <div>
          {data.map((entry, index) => (
            <div className="cache-chart-label">
              <span
                className="color-swatch cache-hit-color-swatch"
                style={{ backgroundColor: getChartColor(index) }}></span>
              <span className="cache-stat">
                <span className="cache-stat-duration">{format.durationUsec(entry.value)}</span>{" "}
                <span className="cache-stat-description">
                  {entry.name} ({format.percent(entry.value / sum.value)}%)
                </span>
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
