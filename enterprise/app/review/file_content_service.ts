import * as monaco from "monaco-editor";
import { CancelablePromise } from "../../../app/util/async";
import { github } from "../../../proto/github_ts_proto";
import rpc_service from "../../../app/service/rpc_service";
import { getLangHintFromFilePath } from "../monaco/monaco";

// Proto interfaces are fully optional, but we need these to all be defined,
// so yeah, an interface.
interface GithubFileDescriptor {
  owner: string;
  repo: string;
  path: string;
  ref: string;
}

const textDecoder = new TextDecoder();

export function getMonacoModelForGithubFile(file: GithubFileDescriptor): CancelablePromise<monaco.editor.ITextModel> {
  const uri = monaco.Uri.file(`${file.path}-${file.ref}`);
  const existingModel = monaco.editor.getModel(uri);
  if (existingModel) {
    return new CancelablePromise(Promise.resolve(existingModel));
  }

  return rpc_service.service.getGithubContent(new github.GetGithubContentRequest(file)).then((r) => {
    // Monaco will get mad if someone made one already.
    const existingModel = monaco.editor.getModel(uri);
    if (existingModel) {
      return existingModel;
    }
    return monaco.editor.createModel(textDecoder.decode(r.content), getLangHintFromFilePath(file.path), uri);
  });
}

// A convenience function for displaying an error in a readonly editor.
export function getModelForText(text: string): monaco.editor.ITextModel {
  const uri = monaco.Uri.file(`bb-goofy-error-thing/${text}`);
  return monaco.editor.getModel(uri) ?? monaco.editor.createModel(text, undefined, uri);
}
