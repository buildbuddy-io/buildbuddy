import React from "react";
import { registry } from "../../../proto/registry_ts_proto";
import { Copy, Info } from "lucide-react";
import { copyToClipboard } from "../../../app/util/clipboard";
import { joinReactNodes } from "../../../app/util/react";
import Link from "../../../app/components/link/link";

interface ImageProps {
    image: registry.Image
}
  
interface ImageState {
}

export default class ImageComponent extends React.Component<ImageProps, ImageState> {
  pullCommand(repo: string, digest: string, baseimage: string) {
    // The baseimage stuff is a hack here to support a regular ole HTTP
    // registry. See, normally when we resume this container from its
    // checkpoint, it requires the base image which cannot be automatically
    // pulled by the "restore" command because the "restore" command does not
    // accept the "--tls-verify=false" flag. So instead we have to know and pull
    // the base image here, as well as the checkpoint. Note that the sever-side
    // calculation of this base image is very incorrect but demo-able.
    if (baseimage === "") {
      return "sudo podman pull --tls-verify=false localhost:8080/" + repo + "@" + digest  
    } else {
      return "sudo podman pull --tls-verify=false localhost:8080/" + repo + "@" + digest + " localhost:8080/" + baseimage
    }
  }

  restoreCommandForCopy(repo: string, digest: string) {
    return "sudo podman container restore --runtime=runc localhost:8080/" + repo + "@" + digest + " && cid=$(sudo podman ps -a -n=1 --format={{.ID}}) && sudo podman start $cid && sudo podman exec --runtime=runc -it $cid /bin/bash"
  }

  handleCopyClicked(s: string) {
    copyToClipboard(s);
  }

  // Inlining this in the <code> block below makes angular barf and we want to
  // bind stuff in that block so we can't make it ng-non-bindable or whatever.
  cidString() {
    return "{{.ID}}"
  }

  render() {
    var pullCmd = this.pullCommand(this.props.image.repository, this.props.image.digest, this.props.image.baseimage)
    return (<div>
          <div className="registry-repo"><b>Repository:</b> {this.props.image.repository}</div>
          <div className="image-digest">{this.props.image.digest}</div>
          <div className="image-tags">
            Tags: {joinReactNodes(this.props.image.tags.map((tag) => <div className="image-tag">{tag}</div>), <div>,</div>)}
          </div>
          <div className="image-size">Size: {this.props.image.size}</div>
          <div className="image-uptime">Uploaded: {this.props.image.uploadedTime}</div>
          <div className="image-latime">Last Accessed: {this.props.image.lastAccessTime}</div>
          <div className="image-accesses">Accesses: {this.props.image.accesses}</div>
          <div className="image-pull">Pull:
            <Copy
              className="copy-icon"
              onClick={this.handleCopyClicked.bind(this, pullCmd)}
              />
              <code>{pullCmd}</code>
          </div>
          {this.props.image.checkpoint &&
            <div className="image-pull">Restore:
              <Copy
                className="copy-icon"
                onClick={this.handleCopyClicked.bind(this, this.restoreCommandForCopy(this.props.image.repository, this.props.image.digest))}
                />
              <code>
                sudo podman container restore --runtime=runc localhost:8080/${this.props.image.repo}@${this.props.image.digest} \<br />
                cid=$(sudo podman ps -a -n=1 --format=${this.cidString()} \<br />
                sudo podman start $cid \<br />
                sudo podman exec --runtime=runc -it $cid /bin/bash
              </code>
            </div>
          }
        </div>
    )
  }
}
