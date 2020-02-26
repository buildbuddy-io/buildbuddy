import React from 'react';
import xterm from 'xterm';
import { FitAddon } from 'xterm-addon-fit';

export interface TerminalProps extends React.DOMAttributes<{}> {
  options?: any;
  value?: string;
}

class LightTheme {
  background = "#fff";
  foreground = "#000";
  selection = "rgba(0, 0, 0, .05)";
}

class DarkTheme {
  background = "#222";
  foreground = "#fff";
  selection = "rgba(255, 255, 255, .1)";
}

export default class TerminalComponent extends React.Component<TerminalProps> {
  fit: FitAddon;
  xterm: xterm.Terminal;
  container: HTMLDivElement;
  constructor(props?: TerminalProps, context?: any) {
    super(props, context);
  }

  componentDidMount() {
    this.xterm = new xterm.Terminal({
      rendererType: "dom",
      convertEol: true,
      lineHeight: 1.4,
      scrollback: 9999999,
      cols: 500,
      theme: new DarkTheme(),
    });
    this.fit = new FitAddon();
    this.xterm.loadAddon(this.fit);

    this.xterm.open(this.container);
    if (this.props.value) {
      this.xterm.write(this.props.value, () => {
        this.fit.fit();
      });
    }
  }
  componentWillUnmount() {
    if (this.xterm) {
      this.xterm.dispose();
      this.xterm = null;
    }
  }

  shouldComponentUpdate(nextProps: any, nextState: any) {
    if (nextProps.hasOwnProperty('value') && nextProps.value != this.props.value) {
      if (this.xterm) {
        this.xterm.resize(500, 20);
        this.xterm.reset();
        setTimeout(() => {
          this.xterm.write(nextProps.value, () => {
            this.fit.fit();
          });
        }, 0);
      }
    } else {
      setTimeout(() => {
        this.fit.fit();
      }, 0);
    }

    return false;
  }

  write(data: any) {
    this.xterm && this.xterm.write(data);
  }

  render() {
    return <div ref={ref => (this.container = ref)} className="terminal" />;
  }
}
export { TerminalComponent };