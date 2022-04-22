import format from "../../format/format";
import React from "react";
import Long from "long";

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
        style={{
          backgroundColor: format.colorHash(props.digest.hash),
          ...(props.hashWidth !== undefined && { width: props.hashWidth }),
        }}>
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

export function parseDigest(value: string): Digest {
  const parts = value.split("/");
  return {
    hash: parts[0],
    sizeBytes: parts.length > 1 ? Number(parts[1]) : null,
  };
}

export default DigestComponent;
