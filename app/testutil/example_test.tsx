import React from "react";
import type { UnaryRpcMethod } from "../service/rpc_service";
import { CancelablePromise } from "../util/async";
import { user_id } from "../../proto/user_id_ts_proto";
import { user } from "../../proto/user_ts_proto";
import { reactSetup, render, screen, waitFor } from "./react";

describe("react component test example", () => {
  reactSetup();

  it("renders data loaded from an RPC", async () => {
    const getUser = jasmine.createSpy("getUser").and.returnValue(
      cancelable(
        new user.GetUserResponse({
          displayUser: new user_id.DisplayUser({
            name: new user_id.Name({ full: "Build Buddy" }),
            email: "buddy@example.com",
          }),
        })
      )
    );

    // The component starts by calling the GetUser RPC.
    render(<ExampleComponent getUser={getUser} />);
    await waitFor(() => expect(getUser).toHaveBeenCalled());
    expect(getUser.calls.mostRecent().args[0]).toEqual(new user.GetUserRequest());

    // The fake response is rendered as a user card.
    await waitFor(() => expect(screen.getByText("Build Buddy")).toBeTruthy());
    expect(screen.getByText("buddy@example.com")).toBeTruthy();
  });
});

interface ExampleComponentProps {
  getUser: UnaryRpcMethod<user.GetUserRequest, user.GetUserResponse>;
}

interface ExampleState {
  displayUser: user_id.DisplayUser | null;
}

class ExampleComponent extends React.Component<ExampleComponentProps, ExampleState> {
  state: ExampleState = { displayUser: null };

  componentDidMount() {
    this.props.getUser(new user.GetUserRequest()).then((response) => {
      this.setState({ displayUser: response.displayUser });
    });
  }

  render() {
    if (!this.state.displayUser) {
      return <div>Loading user...</div>;
    }

    return (
      <div className="user-card">
        <div>{this.state.displayUser.name?.full}</div>
        <div>{this.state.displayUser.email}</div>
      </div>
    );
  }
}

function cancelable<T>(value: T): CancelablePromise<T> {
  return new CancelablePromise(Promise.resolve(value));
}
