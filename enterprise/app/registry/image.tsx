import React from "react";
import { registry } from "../../../proto/registry_ts_proto";
import { Copy, Info } from "lucide-react";
import { copyToClipboard } from "../../../app/util/clipboard";
import Link from "../../../app/components/link/link";

interface ImageProps {
    image: registry.Image
}
  
interface ImageState {
}

export default class ImageComponent extends React.Component<ImageProps, ImageState> {
  pullString(repo: string, digest: string) {
    return "sudo podman pull --tls-verify=false localhost:8080/" + repo + "@" + digest
  }

  restoreString(repo: string, digest: string) {
    return "sudo podman container restore --runtime=runc localhost:8080/" + repo + "@" + digest
  }

  handleCopyClicked(s: string) {
    copyToClipboard(s);
  }

  render() {
    var pullstring = this.pullString(this.props.image.repository, this.props.image.digest)
    var restoreString = this.restoreString(this.props.image.repository, this.props.image.digest)
    return (
        <div>
            <div className="registry-repo"><b>Repository:</b> {this.props.image.repository}</div>
            <div className="image-digest">{this.props.image.digest}</div>
            <div className="image-tags">
                Tags: 
                {this.props.image.tags.map((tag) => <div className="image-tag">{tag}, </div>)}
            </div>
            <div className="image-size">Size: {this.props.image.size}</div>
            <div className="image-uptime">Uploaded: {this.props.image.uploadedTime}</div>
            <div className="image-latime">Last Accessed: {this.props.image.lastAccessTime}</div>
            <div className="image-accesses">Accesses: {this.props.image.accesses}</div>
            <div className="image-pull">Pull:
              <Copy
                className="copy-icon"
                onClick={this.handleCopyClicked.bind(this, pullstring)}
                />
                <code>{pullstring}</code>
            </div>
            {this.props.image.checkpoint &&
              <div className="image-pull">Restore -- doesn't work :-( :
              <Copy
                className="copy-icon"
                onClick={this.handleCopyClicked.bind(this, restoreString)}
                />
                <code>{restoreString}</code>
              </div>}
        </div>
    )
  }
}
