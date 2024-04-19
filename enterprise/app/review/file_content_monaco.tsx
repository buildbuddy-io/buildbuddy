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

    return (
      <MonacoDiffViewerComponent
        handler={this.props.handler}
        originalContent={this.state.originalContent}
        originalThreads={leftThreads}
        modifiedContent={this.state.modifiedContent}
        modifiedThreads={rightThreads}
        disabled={this.props.disabled}
        path={this.props.path}
        baseSha={this.props.baseSha}
        commitSha={this.props.commitSha}
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
  baseSha: string;
  commitSha: string;
}

interface ThreadZoneAndOverlay {
  id: string;
  element: HTMLDivElement;
  overlayElement: HTMLDivElement;
  thread: ThreadModel;
}

interface MonacoDiffViewerComponentState {
  editor?: monaco.editor.IStandaloneDiffEditor;
  originalEditorThreadZones: AutoZone[];
  modifiedEditorThreadZones: AutoZone[];
}

class EditorMouseListener implements monaco.IDisposable {
  private readonly path: string;
  private readonly side: github.CommentSide;
  private readonly commitSha: string;

  private disposables: monaco.IDisposable[];
  private handler: ReviewController;
  private startLine: number;

  constructor(
    path: string,
    side: github.CommentSide,
    commitSha: string,
    editor: monaco.editor.ICodeEditor,
    handler: ReviewController
  ) {
    this.path = path;
    this.side = side;
    this.commitSha = commitSha;
    this.disposables = [];
    this.handler = handler;
    this.startLine = 0;

    this.disposables.push(editor.onMouseDown((e) => this.onMouseDown(e)));
    this.disposables.push(editor.onMouseUp((e) => this.onMouseUp(e)));
    this.disposables.push(editor.onMouseMove((e) => this.onMouseMove(e)));
    this.disposables.push(editor.onMouseMove((e) => this.onMouseLeave(e)));
  }

  onMouseDown(e: monaco.editor.IEditorMouseEvent) {
    if (e.target.position === null) {
      this.startLine = 0;
    } else {
      this.startLine = e.target.position.lineNumber;
    }
  }

  onMouseUp(e: monaco.editor.IEditorMouseEvent) {
    if (e.target.position === null) {
      this.startLine = 0;
    } else {
      const line = e.target.position.lineNumber;
      if (line > 0 && line === this.startLine) {
        // !!! Time to fire.
        this.handler.startComment(this.side, this.path, this.commitSha, this.startLine);
      }
    }
  }

  onMouseMove(e: monaco.editor.IEditorMouseEvent) {
    const currentLine = e.target.position ? e.target.position.lineNumber : 0;
    if (currentLine !== this.startLine) {
      this.startLine = 0;
    }
  }
  onMouseLeave(e: monaco.editor.IEditorMouseEvent) {
    this.startLine = 0;
  }

  dispose() {
    this.disposables.forEach((d) => d.dispose());
    this.disposables = [];
  }
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

  state: MonacoDiffViewerComponentState = {
    originalEditorThreadZones: [],
    modifiedEditorThreadZones: [],
  };

  componentDidMount() {
    // Element is always part of the render() result.
    const container = this.monacoElement.current!;
    const editor = monaco.editor.createDiffEditor(container, {
      automaticLayout: true,
      scrollBeyondLastLine: false,
      scrollbar: {
        alwaysConsumeMouseWheel: false,
        handleMouseWheel: false,
        horizontal: "hidden",
        vertical: "hidden",
      },
      wordWrap: "on",
      wrappingStrategy: "advanced",
      minimap: {
        enabled: false,
      },
      renderLineHighlight: "none",
      renderOverviewRuler: false,
      readOnly: true,
      cursorStyle: "line",
      cursorWidth: 0,
      overviewRulerLanes: 0,
      hideUnchangedRegions: {
        enabled: true,
      },
    });
    this.setState({ editor });

    // XXX: Need to not re-create on back etc.
    editor.setModel({
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
    const listener = new EditorMouseListener(
      this.props.path,
      github.CommentSide.RIGHT_SIDE,
      this.props.commitSha,
      editor.getModifiedEditor(),
      this.props.handler
    );
    const listener2 = new EditorMouseListener(
      this.props.path,
      github.CommentSide.LEFT_SIDE,
      this.props.baseSha,
      editor.getOriginalEditor(),
      this.props.handler
    );

    updateHeight();
  }

  static getDerivedStateFromProps(props: MonacoDiffViewerComponentProps, state: MonacoDiffViewerComponentState) {
    console.log("Deriving state...");
    console.log(props);
    console.log(state);
    const editor = state.editor;
    if (!editor) {
      return;
    }
    const originalUpdates = getAddedAndRemovedThreads(props.originalThreads, state.originalEditorThreadZones);
    const modifiedUpdates = getAddedAndRemovedThreads(props.modifiedThreads, state.modifiedEditorThreadZones);

    if (
      originalUpdates.added.length === 0 &&
      originalUpdates.removed.length === 0 &&
      modifiedUpdates.added.length === 0 &&
      modifiedUpdates.removed.length === 0
    ) {
      // Nothing to update.
      return null;
    }
    const newOriginalZones: AutoZone[] = originalUpdates.kept;
    const newModifiedZones: AutoZone[] = modifiedUpdates.kept;

    editor.getOriginalEditor().changeViewZones(function (changeAccessor) {
      originalUpdates.added.forEach((t) => {
        newOriginalZones.push(AutoZone.create(t.getId(), t.getLine(), editor.getOriginalEditor(), changeAccessor));
      });
    });
    originalUpdates.removed.forEach((z) => z.removeFromEditor());
    editor.getModifiedEditor().changeViewZones(function (changeAccessor) {
      modifiedUpdates.added.forEach((t) => {
        newOriginalZones.push(AutoZone.create(t.getId(), t.getLine(), editor.getModifiedEditor(), changeAccessor));
      });
    });
    modifiedUpdates.removed.forEach((z) => z.removeFromEditor());
    return {
      originalEditorThreadZones: newOriginalZones,
      modifiedEditorThreadZones: newModifiedZones,
    };
  }

  componentDidUpdate() {
    this.state.originalEditorThreadZones.forEach((z) => z.updateHeight());
    this.state.modifiedEditorThreadZones.forEach((z) => z.updateHeight());
  }

  render() {
    // XXX: Iterate through existing zone portals.  if the thread exists, use it.
    // Otherwise, make a new one.
    // And remove all the dead ones, too.
    const zonesToRender = [...this.state.originalEditorThreadZones, ...this.state.modifiedEditorThreadZones];
    const zonePortals = zonesToRender.map((tz) => {
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

function getAddedAndRemovedThreads(
  threads: ThreadModel[],
  existingZones: AutoZone[]
): { added: ThreadModel[]; removed: AutoZone[]; kept: AutoZone[] } {
  const existingZoneSet: Set<string> = new Set();
  existingZones.forEach((z) => existingZoneSet.add(z.threadId));
  const newThreadMap: Map<string, ThreadModel> = new Map();
  threads.forEach((t) => newThreadMap.set(t.getId(), t));

  const added = [...newThreadMap.values()].filter((v) => !existingZoneSet.has(v.getId()));
  const removed = [...existingZones].filter((v) => !newThreadMap.has(v.threadId));
  const kept = existingZones.filter((v) => newThreadMap.has(v.threadId));

  return { added, removed, kept };
}