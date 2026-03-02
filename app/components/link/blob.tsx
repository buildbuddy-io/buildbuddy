import React from "react";

export type WithBlobObjectURLProps = {
  /** Blob parts used to create an object URL. */
  blobParts: BlobPart[];
  /** Optional Blob options such as MIME type. */
  options?: BlobPropertyBag;
  /** Render prop that receives the generated object URL. */
  children: (href: string) => React.ReactNode;
};

interface State {
  href: string;
}

/**
 * Generates a blob-backed object URL and passes it to children via render props.
 *
 * The object URL is revoked whenever inputs change and when the component unmounts.
 */
export default class WithBlobObjectURL extends React.Component<WithBlobObjectURLProps, State> {
  state: State = {
    href: this.createHref(this.props),
  };

  componentDidUpdate(prevProps: WithBlobObjectURLProps) {
    if (prevProps.blobParts === this.props.blobParts && prevProps.options === this.props.options) {
      return;
    }
    window.URL.revokeObjectURL(this.state.href);
    this.setState({ href: this.createHref(this.props) });
  }

  componentWillUnmount() {
    window.URL.revokeObjectURL(this.state.href);
  }

  private createHref(props: WithBlobObjectURLProps): string {
    return window.URL.createObjectURL(new Blob(props.blobParts, props.options));
  }

  render() {
    return this.props.children(this.state.href);
  }
}
