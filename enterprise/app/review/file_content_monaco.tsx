import React from "react";
import { github } from "../../../proto/github_ts_proto";
import ReviewThreadComponent from "./review_thread";
import { CommentModel, ReviewModel, ThreadModel } from "./review_model";
import { ReviewController } from "./review_controller";
import * as monaco from "monaco-editor";
import { createPortal } from "react-dom";
import rpc_service from "../../../app/service/rpc_service";
import error_service from "../../../app/errors/error_service";

interface FileContentMonacoComponentProps {
  reviewModel: ReviewModel;
  disabled: boolean;
  handler: ReviewController;
  viewerLogin: string;
  owner: string;
  repo: string;
  pull: number;
  path: string;
  patch: string;
  baseSha: string;
  commitSha: string;
}

interface FileContentMonacoComponentState {
  originalContent: string;
  modifiedContent: string;
}

const textDecoder = new TextDecoder();

export default class FileContentMonacoComponent extends React.Component<
  FileContentMonacoComponentProps,
  FileContentMonacoComponentState
> {
  state: FileContentMonacoComponentState = {
    originalContent: "",
    modifiedContent: "",
  };
  componentWillMount() {
    // 1. Fetch file content.
    // 2. When content ready, render subcomponent using static values.
    // XXX: Need to cache.
    rpc_service.service
      .getGithubContent(
        new github.GetGithubContentRequest({
          owner: this.props.owner,
          repo: this.props.repo,
          path: this.props.path,
          ref: this.props.commitSha,
        })
      )
      .then((r) => {
        const modifiedContent = textDecoder.decode(r.content);
        this.setState({ modifiedContent });
      })
      .catch((e) => {
        error_service.handleError("Failed to fetch source: " + e);
      });

    rpc_service.service
      .getGithubContent(
        new github.GetGithubContentRequest({
          owner: this.props.owner,
          repo: this.props.repo,
          path: this.props.path,
          ref: this.props.baseSha,
        })
      )
      .then((r) => {
        const originalContent = textDecoder.decode(r.content);
        this.setState({ originalContent });
      })
      .catch((e) => {
        error_service.handleError("Failed to fetch source: " + e);
      });
  }

  render(): JSX.Element {
    if (this.props.patch.length === 0) {
      return <div>No diff info available (binary file?)</div>;
    }
    // XXX: Added / removed files.
    if (!this.state.originalContent || !this.state.modifiedContent) {
      return <div>LOADING</div>;
    }

    const comments = this.props.reviewModel.getCommentsForFile(this.props.path, this.props.commitSha);
    const threads = ThreadModel.threadsFromComments(comments, this.props.reviewModel.getDraftReviewId());
    const leftThreads: ThreadModel[] = [];
    const rightThreads: ThreadModel[] = [];
    threads.forEach((t) => {
      if (t.getSide() === github.CommentSide.LEFT_SIDE) {
        leftThreads.push(t);
      } else {
        rightThreads.push(t);
      }
    });

    console.log(leftThreads);
    console.log(rightThreads);

    return (
      <MonacoDiffViewerComponent
        handler={this.props.handler}
        originalContent={this.state.originalContent}
        originalThreads={leftThreads}
        modifiedContent={this.state.modifiedContent}
        modifiedThreads={rightThreads}
        disabled={this.props.disabled}
        path={this.props.path}
        reviewModel={this.props.reviewModel}></MonacoDiffViewerComponent>
    );
  }
}

interface MonacoDiffViewerComponentProps {
  reviewModel: ReviewModel;
  disabled: boolean;
  handler: ReviewController;
  originalContent: string;
  originalThreads: ThreadModel[];
  modifiedContent: string;
  modifiedThreads: ThreadModel[];
  path: string;
}

interface ThreadZoneAndOverlay {
  id: string;
  element: HTMLDivElement;
  overlayElement: HTMLDivElement;
  thread: ThreadModel;
}

interface MonacoDiffViewerComponentState {
  threadZones: AutoZone[];
}

class AutoZone {
  readonly threadId: string;
  readonly zoneId: string;
  updateFunction: (ca: monaco.editor.IViewZoneChangeAccessor) => void;
  overlayWidget?: monaco.editor.IOverlayWidget;
  editor?: monaco.editor.ICodeEditor;

  // XXX: Is it ok to hold editor ref here?
  constructor(
    threadId: string,
    zoneId: string,
    updateFunction: (ca: monaco.editor.IViewZoneChangeAccessor) => void,
    overlayWidget: monaco.editor.IOverlayWidget,
    editor: monaco.editor.ICodeEditor
  ) {
    this.threadId = threadId;
    this.zoneId = zoneId;
    this.updateFunction = updateFunction;
    this.overlayWidget = overlayWidget;
    this.editor = editor;
  }

  updateHeight() {
    if (!this.editor) {
      return;
    }
    this.editor.changeViewZones((ca) => this.updateFunction(ca));
  }

  removeFromEditor() {
    if (!this.editor || !this.overlayWidget) {
      return;
    }
    this.editor.removeOverlayWidget(this.overlayWidget);
    this.editor.changeViewZones((a) => a.removeZone(this.zoneId));
    this.overlayWidget = undefined;
    this.editor = undefined;
  }

  static create(
    threadId: string,
    line: number,
    editor: monaco.editor.ICodeEditor,
    changeAccessor: monaco.editor.IViewZoneChangeAccessor
  ): AutoZone {
    const zoneElement = document.createElement("div");
    const overlayElement = document.createElement("div");

    overlayElement.classList.add("monaco-thread");

    const overlay: monaco.editor.IOverlayWidget = {
      getId: function () {
        return threadId;
      },
      getDomNode: function () {
        return overlayElement;
      },
      getPosition: function () {
        return null;
      },
    };

    let zoneId: string;
    const updateFunction = (ca: monaco.editor.IViewZoneChangeAccessor) => {
      if (zoneForMonaco.heightInPx !== overlayElement.getBoundingClientRect().height) {
        zoneForMonaco.heightInPx = overlayElement.getBoundingClientRect().height;
        ca.layoutZone(zoneId);
      }
    };
    const zoneForMonaco: monaco.editor.IViewZone = {
      afterLineNumber: line,
      heightInLines: 10,
      domNode: zoneElement,
      onDomNodeTop: (top) => {
        overlayElement.style.top = top + "px";
      },
      onComputedHeight: (_) => {
        editor.changeViewZones(function (ca) {
          updateFunction(ca);
        });
      },
    };
    zoneId = changeAccessor.addZone(zoneForMonaco);
    editor.addOverlayWidget(overlay);

    return new AutoZone(threadId, zoneId, updateFunction, overlay, editor);
  }
}

class MonacoDiffViewerComponent extends React.Component<
  MonacoDiffViewerComponentProps,
  MonacoDiffViewerComponentState
> {
  monacoElement: React.RefObject<HTMLDivElement> = React.createRef();
  editor: monaco.editor.IDiffEditor | undefined;

  state: MonacoDiffViewerComponentState = {
    threadZones: [],
  };

  insertThreads() {
    const { props, editor } = this;
    console.log("aaa");
    if (!editor) {
      return;
    }

    // 1: Compute editor available area.
    // 2: Buffer comments off-screen to compute height.
    // 3: Create zones and overlay widgets, attach.
    const newThreadZones: AutoZone[] = [];
    console.log("bbb");
    // XXX: Extract fn..
    editor.getOriginalEditor().changeViewZones(function (changeAccessor) {
      props.originalThreads.forEach((t) => {
        console.log("Yes");
        console.log(t.getId() + " " + t.getLine());
        newThreadZones.push(AutoZone.create(t.getId(), t.getLine(), editor.getOriginalEditor(), changeAccessor));
      });
    });

    console.log("ccc");

    editor.getModifiedEditor().changeViewZones(function (changeAccessor) {
      props.modifiedThreads.forEach((t) => {
        console.log("Yes");
        console.log(t.getId() + " " + t.getLine());
        newThreadZones.push(AutoZone.create(t.getId(), t.getLine(), editor.getModifiedEditor(), changeAccessor));
      });
    });

    this.setState({ threadZones: newThreadZones });
  }

  componentDidMount() {
    // Element is always part of the render() result.
    const container = this.monacoElement.current!;
    const editor = monaco.editor.createDiffEditor(container, {
      automaticLayout: true,
      scrollBeyondLastLine: false,
      scrollbar: {
        alwaysConsumeMouseWheel: false,
      },
      wordWrap: "on",
      wrappingStrategy: "advanced",
      minimap: {
        enabled: false,
      },
      renderOverviewRuler: false,
      readOnly: true,
      hideUnchangedRegions: {
        enabled: true,
      },
    });
    this.editor = editor;

    this.editor.setModel({
      original: monaco.editor.createModel(
        this.props.originalContent,
        undefined,
        monaco.Uri.file(`original-${this.props.path}`)
      ),
      modified: monaco.editor.createModel(
        this.props.modifiedContent,
        undefined,
        monaco.Uri.file(`modified-${this.props.path}`)
      ),
    });

    let ignoreEvent = false;
    const maxHeight = () => {
      return Math.max(editor.getOriginalEditor().getContentHeight(), editor.getModifiedEditor().getContentHeight());
    };
    const trueWidth = () => {
      console.log(editor);
      return editor.getContainerDomNode().getBoundingClientRect().width;
    };
    const updateHeight = () => {
      if (ignoreEvent) {
        return;
      }
      const contentHeight = maxHeight();
      container.style.height = `${contentHeight}px`;
      try {
        ignoreEvent = true;
        editor.getModifiedEditor().layout({ width: trueWidth() / 2, height: contentHeight });
        editor.getOriginalEditor().layout({ width: trueWidth() / 2, height: contentHeight });
      } finally {
        ignoreEvent = false;
      }
    };
    editor.getOriginalEditor().onDidContentSizeChange(updateHeight);
    editor.getModifiedEditor().onDidContentSizeChange(updateHeight);

    console.log("Hi there");
    this.insertThreads();

    updateHeight();
  }

  componentDidUpdate() {
    this.state.threadZones.forEach((z) => z.updateHeight());
  }

  render() {
    // XXX: Iterate through existing zone portals.  if the thread exists, use it.
    // Otherwise, make a new one.
    // And remove all the dead ones, too.
    console.log("ZONES!");
    console.log(this.props);
    const zonePortals = this.state.threadZones.map((tz) => {
      const thread =
        this.props.modifiedThreads.find((t) => t.getId() === tz.threadId) ??
        this.props.originalThreads.find((t) => t.getId() === tz.threadId);
      if (thread === undefined) {
        // It's gone!
        // XXX: Remove
        return undefined;
      }
      const portalRoot = tz.overlayWidget?.getDomNode();
      if (!portalRoot) {
        return undefined;
      }

      const comments = thread.getComments();
      const draft = thread.getDraft();
      console.log("Creating");
      console.log(draft);

      return createPortal(
        <ReviewThreadComponent
          threadId={thread.getId()}
          reviewId={this.props.reviewModel.getDraftReviewId()}
          viewerLogin={this.props.reviewModel.getViewerLogin()}
          comments={comments}
          draftComment={draft}
          disabled={Boolean(this.props.disabled)}
          updating={Boolean(draft && draft.isSubmittedToGithub())}
          editing={Boolean(this.props.reviewModel.isCommentInProgress(draft?.getId()))}
          saving={/* TODO(jdhollen */ false}
          handler={this.props.handler}
          activeUsername={this.props.reviewModel.getViewerLogin()}></ReviewThreadComponent>,
        portalRoot
      );
    });

    return (
      <>
        <div style={{ width: "100%", height: "300px" }} ref={this.monacoElement}></div>
        {zonePortals}
      </>
    );
  }
}
