import { build } from "../../proto/remote_execution_ts_proto";
import { REDACTED_PLACEHOLDER, redactCommand } from "./invocation_action_card_redaction";

describe("InvocationActionCard redaction helpers", () => {
  it("redacts env vars not present in the allowlist", () => {
    const command = build.bazel.remote.execution.v2.Command.create({
      environmentVariables: [
        build.bazel.remote.execution.v2.Command.EnvironmentVariable.create({
          name: "SECRET_ENV",
          value: "super-secret",
        }),
        build.bazel.remote.execution.v2.Command.EnvironmentVariable.create({
          name: "SAFE_ENV",
          value: "safe-value",
        }),
        build.bazel.remote.execution.v2.Command.EnvironmentVariable.create({
          name: "USER",
          value: "ci-bot",
        }),
      ],
    });

    const redactedCommand = redactCommand(command, { allowEnvMetadata: "SAFE_ENV" });

    const secret = redactedCommand.environmentVariables?.find((variable) => variable?.name === "SECRET_ENV");
    const allowed = redactedCommand.environmentVariables?.find((variable) => variable?.name === "SAFE_ENV");
    const defaultAllowed = redactedCommand.environmentVariables?.find((variable) => variable?.name === "USER");

    expect(secret?.value).toBe(REDACTED_PLACEHOLDER);
    expect(allowed?.value).toBe("safe-value");
    expect(defaultAllowed?.value).toBe("ci-bot");
    expect(command.environmentVariables?.find((variable) => variable?.name === "SECRET_ENV")?.value).toBe(
      "super-secret"
    );
  });

  it("supports wildcard entries in the allowlist metadata", () => {
    const command = build.bazel.remote.execution.v2.Command.create({
      environmentVariables: [
        build.bazel.remote.execution.v2.Command.EnvironmentVariable.create({
          name: "TOKEN_API",
          value: "api-token",
        }),
        build.bazel.remote.execution.v2.Command.EnvironmentVariable.create({
          name: "TOKEN_SECRET",
          value: "another-secret",
        }),
        build.bazel.remote.execution.v2.Command.EnvironmentVariable.create({
          name: "OTHER_SECRET",
          value: "super-secret",
        }),
      ],
    });

    const redactedCommand = redactCommand(command, { allowEnvMetadata: "TOKEN_*" });

    const tokenApi = redactedCommand.environmentVariables?.find((variable) => variable?.name === "TOKEN_API");
    const tokenSecret = redactedCommand.environmentVariables?.find((variable) => variable?.name === "TOKEN_SECRET");
    const otherSecret = redactedCommand.environmentVariables?.find((variable) => variable?.name === "OTHER_SECRET");

    expect(tokenApi?.value).toBe("api-token");
    expect(tokenSecret?.value).toBe("another-secret");
    expect(otherSecret?.value).toBe(REDACTED_PLACEHOLDER);
  });
});
