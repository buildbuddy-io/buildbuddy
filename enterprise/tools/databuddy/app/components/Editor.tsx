import React from "react";
import * as monaco from "monaco-editor";
import { useEffect, useRef } from "react";
import { configureMonaco } from "../monaco_config";

configureMonaco(() => (window as any)._tableSchemas);

export type EditorProps = {
  content: string;
  language: string;
  readonly?: boolean;
  onChange: (value: string) => void;
  onSave: (value: string) => void;
  onExecute: (value: string) => void;
};

/**
 * Monaco editor component.
 */
export default function Editor(props: EditorProps) {
  // Use a ref for props so that we don't re-create the editor when props change.
  // Re-creating the editor is expensive so we want to avoid this.
  const propsRef = useRef(props);
  useEffect(() => {
    propsRef.current = props;
  }, [props]);

  const editorRef = useRef<monaco.editor.IStandaloneCodeEditor | null>(null);
  const editorElementRef = useRef<HTMLDivElement>(null);

  // Mount monaco editor
  useEffect(() => {
    if (!editorElementRef.current) return;

    const editor = monaco.editor.create(editorElementRef.current!, {
      value: propsRef.current!.content,
      theme: "vs",
      language: propsRef.current!.language,
      minimap: { enabled: false },
      automaticLayout: true,
      readOnly: propsRef.current!.readonly,
    });

    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => {
      propsRef.current!.onSave?.(editor.getValue());
    });
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      propsRef.current!.onExecute?.(editor.getValue());
    });
    editor.onDidChangeModelContent((e) => {
      propsRef.current!.onChange?.(editor.getValue());
    });
    editorRef.current = editor;

    return () => {
      editor.dispose();
    };
  }, []);

  return <div ref={editorElementRef} className="editor-container" />;
}
