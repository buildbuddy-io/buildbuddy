import Long from "long";
import React from "react";
import format from "../../format/format";

export type Digest = {
  hash?: string;
  sizeBytes?: number | Long | null;
};

export type DigestProps = {
  digest: Digest;
  expanded?: boolean;
  expandOnHover?: boolean;
  hashWidth?: string;
  sizeWidth?: string;
};

export const DigestComponent = React.forwardRef((props: DigestProps, ref: React.Ref<HTMLInputElement>) => {
  return (
    <span
      className={`digest-component ${
        props.expandOnHover === undefined || props.expandOnHover ? "expand-on-hover" : ""
      } ${props.expanded ? "expanded" : ""}`}
      ref={ref}>
      <span
        className={`digest-component-hash ${props.hashWidth !== undefined ? "fixed-width" : ""}`}
        title={props.digest.hash}
        style={
          {
            "--digest-hue": format.colorHashHue(props.digest.hash || ""),
            ...(props.hashWidth !== undefined && { width: props.hashWidth }),
          } as React.CSSProperties
        }>
        {props.digest.hash}
      </span>
      {props.digest.sizeBytes !== null && props.digest.sizeBytes !== undefined && (
        <span
          title={`${props.digest.sizeBytes}`}
          className={`digest-component-size ${props.sizeWidth !== undefined ? "fixed-width" : ""}`}
          style={{
            ...(props.sizeWidth !== undefined && { width: props.sizeWidth }),
          }}>
          {format.bytes(props.digest.sizeBytes)}
        </span>
      )}
    </span>
  );
});

export default DigestComponent;
