import format from "../../format/format";
import React from "react";
import Long from "long";

export type DigestProps = { digest: { hash?: string; sizeBytes?: number | Long }; expanded?: boolean };

export const DigestComponent = React.forwardRef((props: DigestProps, ref: React.Ref<HTMLInputElement>) => {
  return (
    <span className={`digest-component ${props.expanded ? "expanded" : ""}`} ref={ref}>
      <span
        className="digest-component-hash"
        title={props.digest.hash}
        style={{ backgroundColor: format.colorHash(props.digest.hash) }}>
        {props.digest.hash}
      </span>
      <span title={`${props.digest.sizeBytes}`} className="digest-component-size">
        {format.bytes(props.digest.sizeBytes)}
      </span>
    </span>
  );
});
export default DigestComponent;
