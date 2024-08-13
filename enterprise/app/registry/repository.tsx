import React from "react";
import { registry } from "../../../proto/registry_ts_proto";
import Link from "../../../app/components/link/link";
import { ChevronsUpDown, File } from "lucide-react";
import { OutlinedButton } from "../../../app/components/button/button";

interface RepositoryProps {
  repository: registry.Repository;
}

interface RepositoryState {}

export default class RepositoryComponent extends React.Component<RepositoryProps, RepositoryState> {
  render() {
    return (
      <div>
        <div className="registry-repo">
          <b>Repository:</b> {this.props.repository.name}
        </div>
        <table className="registry-image-table">
          <tr className="registry-image-header-row">
            <th className="registry-image-header registry-image-digest-header">Digest</th>
            <th className="registry-image-header registry-image-tags-header">Tags</th>
            <th className="registry-image-header registry-image-size-header">Size</th>
            <th className="registry-image-header registry-image-uploaded-header">Uploaded</th>
            <th className="registry-image-header registry-image-laccess-header">Last Accessed</th>
            <th className="registry-image-header registry-image-accesses-header">Accesses</th>
          </tr>
          {this.props.repository.images.map(
            (image) => !image.checkpoint && <ImageComponent image={image}></ImageComponent>
          )}
        </table>
      </div>
    );
  }
}

interface ImageProps {
  image: registry.Image;
}

class ImageComponent extends React.Component<ImageProps> {
  render() {
    return (
      <tr className="registry-image-row">
        <td className="registry-image-digest">
          <Link href={`/registry/?image=${this.props.image.fullname}`}>{this.props.image.digest}</Link>
        </td>
        <td className="registry-image-tags">
          {this.props.image.tags.map((tag) => (
            <div className="registry-image-tag">{tag}</div>
          ))}
        </td>
        <td className="registry-image-size">{this.props.image.size}</td>
        <td className="registry-image-uptime">{this.props.image.uploadedTime}</td>
        <td className="registry-image-atime">{this.props.image.lastAccessTime}</td>
        <td className="registry-image-accesses">{this.props.image.accesses}</td>
      </tr>
    );
  }
}
