import React from "react";
import { registry } from "../../../proto/registry_ts_proto";
import Link from "../../../app/components/link/link";

interface ImageProps {
    image: registry.Image
}
  
interface ImageState {
}

export default class ImageComponent extends React.Component<ImageProps, ImageState> {
  render() {
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
        </div>
    )
  }
}
