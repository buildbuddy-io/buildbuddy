export type Line = {
  jsonPayload?: JSONPayload;
  resource: Resource;
  labels: Labels;
  severity: Severity;
  sourceLocation: SourceLocation;

  textPayload?: string;
  timestamp?: string;

  receiveTimestamp: string;
  insertId: string;

  computed?: ComputedProperties;

  _jsonStringifyCache?: string;
};

export type ComputedProperties = {
  podShortName?: string;
  tags?: string[];
};

export type Severity = "DEBUG" | "INFO" | "WARNING" | "ERROR";

export type JSONPayload = {
  message?: string;
  timestamp: string;

  executor_id?: string;
  executor_host_id?: string;
  execution_id?: string;
  invocation_id?: string;
  request_id?: string;
};

export type Labels = {
  "k8s-pod/app": string;
  "compute.googleapis.com/resource_name": string;
};

export type Resource = {
  type: string;
  labels: ResourceLabels;
};

export type ResourceLabels = {
  pod_name: string;
  container_name: string;
  project_id: string;
  cluster_name: string;
  namespace_name: string;
  location: string;
};

export type SourceLocation = {
  file: string;
  line: string;
};
