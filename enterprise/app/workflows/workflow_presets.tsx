import { Shuffle } from "lucide-react";
import React from "react";

// A WorkflowPreset is a pre-configured check that a user can run on a remote
// runner or wire into their buildbuddy.yaml.
export interface WorkflowPreset {
  // Stable identifier; used as the React key and to track per-preset UI state.
  id: string;
  // Short label shown in the list and as the expanded section header.
  label: string;
  // Icon shown next to the label.
  icon: React.ComponentType<{ className?: string }>;
  // One-line summary shown under the label in both collapsed and expanded states.
  description: string;
  // Human-readable name shown for the remote-run action.
  actionName: string;
  // The `bb` CLI invocation that runs the check.
  cliCommand: string;
  // Complete buildbuddy.yaml snippet shown by the "Copy buildbuddy.yaml" button.
  yaml: string;
  // Optional sub-label shown under the "Copy buildbuddy.yaml" button.
  yamlHint?: string;
}

const NONDETERMINISM: WorkflowPreset = {
  id: "nondeterminism",
  label: "Nondeterminism check",
  icon: Shuffle,
  description: "Runs your build twice with caching disabled and compares results.",
  actionName: "Nondeterminism check",
  cliCommand: "bb detect nondeterminism",
  yaml: `actions:
  - name: Nondeterminism check
    triggers:
      schedule:
        crons:
          - "0 8 * * *"
    steps:
      - run: bb detect nondeterminism
    platform_properties:
      # Caching is disabled anyway, so the value from recycling is minimized.
      recycle-runner: false
`,
  yamlHint: "Schedule a daily run on Workflows",
};

export const WORKFLOW_PRESETS: WorkflowPreset[] = [NONDETERMINISM];
