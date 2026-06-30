import React from "react";
import { User } from "../../../app/auth/user";
import type { BuildBuddyServiceRpcName } from "../../../app/service/rpc_service";
import { reactSetup, render, screen, waitFor } from "../../../app/testutil/react";
import { CancelablePromise } from "../../../app/util/async";
import { api_key } from "../../../proto/api_key_ts_proto";
import { capability } from "../../../proto/capability_ts_proto";
import { grp } from "../../../proto/group_ts_proto";
import { user_id } from "../../../proto/user_id_ts_proto";
import ApiKeys from "./api_keys";
import type { ApiKeysComponentProps } from "./api_keys";

describe("ApiKeysComponent", () => {
  reactSetup();

  it("renders API keys loaded from the get RPC", async () => {
    const getApiKeys = jasmine.createSpy("getApiKeys").and.returnValue(
      cancelable(
        api_key.GetApiKeysResponse.create({
          apiKey: [
            api_key.ApiKey.create({
              id: "key-1",
              label: "CI token",
              capability: [capability.Capability.CACHE_WRITE],
            }),
          ],
        })
      )
    );

    render(<ApiKeys {...newProps({ get: getApiKeys })} />, { container: document.getElementById("app")! });

    await waitFor(() => expect(getApiKeys).toHaveBeenCalled());
    expect(getApiKeys.calls.mostRecent().args[0].groupId).toBe("group-1");
    await waitFor(() => expect(screen.getByText("CI token")).toBeTruthy());
    expect(screen.getByText("Read+Write")).toBeTruthy();
  });
});

function newProps(overrides: Partial<ApiKeysComponentProps> = {}): ApiKeysComponentProps {
  return {
    user: newUser(),
    get: () => cancelable(api_key.GetApiKeysResponse.create()),
    create: () => cancelable(api_key.CreateApiKeyResponse.create()),
    update: () => cancelable(api_key.UpdateApiKeyResponse.create()),
    delete: () => cancelable(api_key.DeleteApiKeyResponse.create()),
    ...overrides,
  };
}

function newUser(): User {
  const selectedGroup = new grp.Group({ id: "group-1", name: "BuildBuddy" });
  return new User({
    displayUser: new user_id.DisplayUser({
      userId: new user_id.UserId({ id: "user-1" }),
      email: "dev@example.com",
    }),
    groups: [selectedGroup],
    selectedGroup,
    allowedRpcs: new Set<BuildBuddyServiceRpcName>(["createApiKey", "updateApiKey", "deleteApiKey"]),
    githubLinked: false,
    isImpersonating: false,
    subdomainGroupID: "",
    codesearchAllowed: false,
  });
}

function cancelable<T>(value: T): CancelablePromise<T> {
  return new CancelablePromise(Promise.resolve(value));
}
