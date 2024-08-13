import React from "react";
import rpcService from "../../../app/service/rpc_service";
import errorService from "../../../app/errors/error_service";
import { search } from "../../../proto/search_ts_proto";
import Spinner from "../../../app/components/spinner/spinner";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import TextInput from "../../../app/components/input/input";
import FilledButton from "../../../app/components/button/button";
import router from "../../../app/router/router";
import shortcuts, { KeyCombo } from "../../../app/shortcuts/shortcuts";
import ResultComponent from "./result";
import { Bird, Search } from "lucide-react";

interface State {
  loading: boolean;
  inputText: string;
}

interface Props {
  path: string;
  search: URLSearchParams;
}

export default class RegistryComponent extends React.Component<Props, State> {
  render() {
    return (
      <div className="iain">hello</div>
    );
  }
}
