import React from "react";
import LinkButton, { LinkButtonProps } from "../components/button/link_button";
import WithBlobObjectURL from "../components/link/blob";

export type CertificateDownloadLinkProps = Omit<LinkButtonProps, "href" | "download"> & {
  filename: string;
  contents: string;
};

export default function CertificateDownloadLink(props: CertificateDownloadLinkProps) {
  const { filename, contents, ...rest } = props;
  return (
    <WithBlobObjectURL blobParts={[contents]} options={{ type: "text/plain" }}>
      {(href) => <LinkButton {...rest} download={filename} href={href} />}
    </WithBlobObjectURL>
  );
}
