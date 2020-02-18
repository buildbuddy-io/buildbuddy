import React from 'react';
import rpcService from 'buildbuddy/app/service/rpc_service'
import { invocation } from 'buildbuddy/proto/invocation_ts_proto';
import { build_event_stream } from 'buildbuddy/proto/build_event_stream_ts_proto';

interface State {
  progress: build_event_stream.Progress[]
}

export default class HomeComponent extends React.Component {
  state: State = {
    progress: []
  };

  componentWillMount() {
    let request = new invocation.GetInvocationRequest()
    rpcService.service.getInvocation(request).then((response) => {
      for (let invocation of response.invocation) {
        for (let buildEvent of invocation.buildEvent) {
          if (buildEvent.progress) this.state.progress.push(buildEvent.progress as build_event_stream.Progress);
        }
      }
      this.setState(this.state);
    });
  }

  render() {
    return (
      <div>
        <div className="shelf">
          <div className="container">
            <div className="titles">
              <div className="title">Siggi's build</div>
              <div className="subtitle"><b>February 16, 2019</b> at <b>10:33 am</b></div>
            </div>
            <div className="details">
              <b>Failed</b> in <b>40 minutes</b> and <b>0 seconds</b><br />
              <b>329</b> targets  &middot;  <b>317</b> passed  &middot;  <b>12</b> failed <br />
            </div>
          </div>
        </div>
        <div className="container">

          <div className="card">
            <img className="icon" src="/image/log-circle.svg" />
            <div className="content">
              <div className="title">Build log</div>
              <div className="details">
                {this.state.progress}
              </div>
              <div className="more">See more</div>
            </div>
          </div>

          <div className="card">
            <img className="icon" src="/image/x-circle.svg" />
            <div className="content">
              <div className="title">12 targets failed</div>
              <div className="details">
              </div>
              <div className="more">See more</div>
            </div>
          </div>

          <div className="card">
            <img className="icon" src="/image/check-circle.svg" />
            <div className="content">
              <div className="title">317 targets passed</div>
              <div className="details">
              </div>
              <div className="more">See more</div>
            </div>
          </div>

          <div className="card">
            <img className="icon" src="/image/info.svg" />
            <div className="content">
              <div className="title">Invocation details</div>
              <div className="details">
              </div>
              <div className="more">See more</div>
            </div>
          </div>

          <div className="card">
            <img className="icon" src="/image/arrow-down-circle.svg" />
            <div className="content">
              <div className="title">Artifacts</div>
              <div className="details">
              </div>
              <div className="more">See more</div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
