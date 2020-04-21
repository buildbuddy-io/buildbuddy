import React from 'react';
import pako from 'pako';
import InvocationModel from './invocation_model';
import CacheCodeComponent from '../docs/cache_code';

interface Props {
  model: InvocationModel,
}

interface State {
  numPages: number;
  profile: any;
  threadMap: Map<number, Thread>;
  sortFunction: (a: string, b: string) => number;
  timingEnabled: boolean;
  buildInProgress: boolean;
}

interface Thread {
  id: number;
  totalDuration: number;
  name: string;
  events: any[];
}

export default class ArtifactsCardComponent extends React.Component {
  props: Props;

  state: State = {
    numPages: 1,
    profile: null,
    threadMap: new Map<number, Thread>(),
    sortFunction: this.sortTimeAsc,
    timingEnabled: true,
    buildInProgress: false
  }

  componentDidMount() {
    this.fetchProfile();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.model !== prevProps.model) {
      this.fetchProfile();
    }
  }

  fetchProfile() {
    let profileUrl = this.props.model.buildToolLogs?.log.find((log: any) => log.name == "command.profile.gz")?.uri;

    if (!profileUrl) {
      this.setState({ ...this.state, buildInProgress: true });
      return;
    }

    if (!profileUrl.startsWith("bytestream://")) {
      this.setState({ ...this.state, timingEnabled: false, buildInProgress: false });
      return;
    }

    if (this.state.profile) {
      // Already fetched
      return;
    }

    var request = new XMLHttpRequest();
    request.responseType = "arraybuffer";
    request.open('GET', "/file/download?filename=trace.gz&bytestream_url=" + encodeURIComponent(profileUrl), true);

    // TODO(siggisim): Do something more robust here
    var isSafari = /constructor/i.test(window.HTMLElement as any) || (function (p) { return p.toString() === "[object SafariRemoteNotification]"; })(!(window as any).safari || (typeof (window as any).safari !== 'undefined' && (window as any).safari.pushNotification));

    let card = this;
    request.onload = function () {
      if (this.status >= 200 && this.status < 400) {
        // Safari doesn't automatically ungzip 1 layer like Chrome and Firefox do for some reason.
        let response = isSafari ? pako.inflate(new Uint8Array(this.response)) : this.response;
        let decompressedResponse = pako.inflate(response, { to: 'string' });
        card.updateProfile(JSON.parse(decompressedResponse));
      } else {
        console.error("Error loading bytestream timing profile!");
      }
    };

    request.onerror = function () {
      console.error("Error loading bytestream timing profile!");
    };

    request.send();
  }

  updateProfile(profile: any) {
    console.log(profile);
    this.state.profile = profile
    for (let event of this.state.profile?.traceEvents || []) {
      let thread = this.state.threadMap.get(event.tid) || { name: "", totalDuration: 0, id: event.tid, events: [] as any[] };

      if (event.ph == "X") { // Duration events
        thread.events.push(event);
        thread.totalDuration += event.dur;
      } else if (event.ph == "M" && event.name == "thread_name") { // Metadata events
        thread.name = event.args.name;
      }

      this.state.threadMap.set(event.tid, thread);
    }
    this.state.buildInProgress = false;
    this.setState(this.state);
  }

  sortIdAsc(a: any, b: any) {
    return a.id - b.id;
  }

  sortDurationDesc(a: any, b: any) {
    return b.dur - a.dur;
  }

  sortTimeAsc(a: any, b: any) {
    return a.ts - b.ts;
  }

  handleStartTimeClicked() {
    this.setState({ ...this.state, sortFunction: this.sortTimeAsc });
  }

  handleDurationClicked() {
    this.setState({ ...this.state, sortFunction: this.sortDurationDesc });
  }

  render() {
    return <div className="card artifacts">
      <img className="icon" src="/image/clock-regular.svg" />
      <div className="content">
        <div className="title">Timing</div>
        <div className="sort-control">Order by <u onClick={this.handleStartTimeClicked.bind(this)} className={`clickable ${this.state.sortFunction == this.sortTimeAsc && 'selected'}`}>start time</u> | <u onClick={this.handleDurationClicked.bind(this)} className={`clickable ${this.state.sortFunction == this.sortDurationDesc && 'selected'}`}>duration</u></div>
        {this.state.buildInProgress && <div className="empty-state">Build is in progress...</div>}
        {!this.state.timingEnabled &&
          <div className="empty-state">
            Profiling isn't enabled for this invocation.<br /><br />To enable profiling you must add GRPC remote caching. You can do so by adding the following line to your <b>.bazelrc</b> and re-running your invocation:
            <CacheCodeComponent />
          </div>
        }
        <div className="details">
          {Array.from(this.state.threadMap.values()).sort(this.sortIdAsc).map((thread: Thread) =>
            <div>
              <div className="list-title">
                <div>{thread.name}</div>
              </div>
              <ul>
                {thread.events.sort(this.state.sortFunction).map((event) =>
                  <li>
                    <div className="list-grid">
                      <div>{event.name}</div>
                      <div>{(event.dur / 1000000).toPrecision(3)} seconds</div>
                    </div>
                    <div className="list-percent" data-percent={`${(100 * (event.dur / this.props.model.getDurationMicros())).toFixed(0)}%`} style={{ width: `${(100 * (event.dur / this.props.model.getDurationMicros())).toPrecision(3)}%` }}></div>
                  </li>
                )}
              </ul>
            </div>
          )}
        </div>
      </div>
    </div>
  }
}
