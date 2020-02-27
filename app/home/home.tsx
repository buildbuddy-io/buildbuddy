import React from 'react';

interface State {
  menuExpanded: boolean;
}

export default class HomeComponent extends React.Component {
  state: State = {
    menuExpanded: false
  };

  componentWillMount() {
    document.title = `Home | Buildbuddy`;
  }

  handleMenuClicked() {
    this.setState({ menuExpanded: !this.state.menuExpanded });
  }

  render() {
    return (
      <div className="home">
        <div className="container">
          <div className="title">Welcome to BuildBuddy!</div>
          <p>
          BuildBuddy is an open source Bazel build event viewer. It helps you collect, view, share and debug build events in a user-friendly web UI.
          </p>
          <p>
            To get started, add the following two lines to your .bazelrc file. If you don't have a .bazelrc file - create one in the same directory as your Bazel WORKSPACE file with the two following lines:
          </p>
          <p>
            <b>.bazelrc</b>
            <code>
              build --bes_results_url={window.location.protocol}//{window.location.host}/invocation/<br/>
              build --bes_backend=grpc://{window.location.hostname.replace("app.", "events.")}:1985
            </code>
          </p>
          <p>
            Once you've added those two lines to your .bazelrc - you'll get a BuildBuddy url printed at the beginning and the end of every Bazel invocation like this:
          </p>
          <p>
            <b>Bazel invocation</b>
            <code>
            $ bazel build //...<br/>
            INFO: Streaming build results to: {window.location.protocol}//{window.location.host}/invocation/7bedd84e-525e-4b93-a5f5-53517d57752b<br/>
            ...
            </code>
          </p>
          <p>
            Now you can command click / double click on these urls to see the results of your build!
          </p>
          <p>
            Feel free to reach out to <a href="mailto:help@tryflame.com">help@tryflame.com</a> if you have any questions or feature requests.
          </p>
        </div>
      </div>
    );
  }
}
