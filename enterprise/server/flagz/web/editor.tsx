import React from "react";
import ReactDOM from "react-dom";
import rpcService from "../../../../app/service/rpc_service";
import { flagz } from "../../../../proto/flagz_ts_proto";
import { BuildBuddyError } from "../../../../app/util/errors";
import authService from "../../../../app/auth/auth_service";
import { User } from  "../../../../app/auth/auth_service";

/*
import * as monaco from "monaco-editor";

// @ts-ignore
self.MonacoEnvironment = {
  getWorkerUrl: function (workerId: string, label: string) {
    // TODO(siggisim): add language support i.e.
    //https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.25.2/min/vs/basic-languages/go/go.min.js
    return `data:text/javascript;charset=utf-8,${encodeURIComponent(`
      self.MonacoEnvironment = {
        baseUrl: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.25.2/min/'
      };
      importScripts('https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.25.2/min/vs/base/worker/workerMain.js');`)}`;
  },
};
*/

interface State {
  user: User;
  loading: boolean;
  error: BuildBuddyError | null;
}

interface Props {
  flags: string;
}

class Editor extends React.Component<Props, State> {
  state: State = {
		user: null,
    loading: true,
    error: null,
  };

	props: Props = {
    flags: "",
  };
  private rootRef = React.createRef<HTMLDivElement>();
	private textAreaRef = React.createRef<HTMLTextAreaElement>();
  // private editor: monaco.editor.ICodeEditor;

  fetchFlags() {
    let request = new flagz.GetFlagzRequest();
    rpcService.service
      .getFlagz(request)
      .then((response: flagz.GetFlagzResponse) => {
        console.log(response);
				this.props.flags = new TextDecoder().decode(response.yamlConfig)
        this.setState({
          loading: false,
          error: null,
        });
				this.textAreaRef.current.value = this.props.flags
      })
      .catch((error: any) => {
        console.error(error);
				this.props.flags = ""
        this.setState({
          error: BuildBuddyError.parse(error),
          loading: false,
        });
      });
  }

	pushFlags() {
    let request = new flagz.SetFlagzRequest();
		this.props.flags = this.textAreaRef.current.value
		request.yamlUpdate = new TextEncoder().encode(this.props.flags)
    rpcService.service
      .setFlagz(request)
      .then((response: flagz.SetFlagzResponse) => {
        console.log(response);
      })
      .catch((error: any) => {
        console.error(error);
        this.setState({
          error: BuildBuddyError.parse(error),
          loading: false,
        });
      });
	}

  componentWillMount() {
    // authService.register();
    // authService.userStream.subscribe({
      // next: (user: User) => this.setState({ ...this.state, user }),
    // });
		this.fetchFlags()
	}

  componentDidMount() {
    // this.editor = monaco.editor.create(this.rootRef.current, {});
  }

  render() {
    // return <div ref={this.rootRef} className="editor-root" />;
    return <div><textarea ref={this.textAreaRef} className="editor-root" cols={120} rows={40}/> <br/> <button onClick={this.pushFlags.bind(this)}>Push</button></div>;
  }
}

ReactDOM.render(<Editor />, document.getElementById("root") as HTMLElement);
