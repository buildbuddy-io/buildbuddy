import React from "react";
import { CheckCircle, Ellipsis, XCircle } from "lucide-react";
import { useEffect, useState } from "react";

export type Status = "ok" | "loading" | "error";

export type Options = {
  status: Status;
};

const toastTarget = new EventTarget();

export default function toast(content: string | null, { status = "ok" }: Partial<Options> = {}) {
  toastTarget.dispatchEvent(
    new CustomEvent<ToastEventDetail>("toast", {
      detail: {
        content: content ?? "",
        status,
      },
    })
  );
}

type ToastEventDetail = {
  content: string;
  status: Status;
  /** Display duration in seconds. */
  duration?: number;
};

export function Toast() {
  const [content, setContent] = useState("");
  const [visible, setVisible] = useState(false);
  const [hideTimeout, setHideTimeout] = useState(0);
  const [status, setStatus] = useState<Status | null>(null);

  useEffect(() => {
    const listener = (e: Event) => {
      const event = e as CustomEvent<ToastEventDetail>;
      const { content, status, duration } = event.detail;
      setVisible(!!content);
      clearTimeout(hideTimeout);
      if (content) {
        setContent(content);
        setStatus(status);
        setHideTimeout(
          window.setTimeout(
            () => {
              setVisible(false);
            },
            duration ?? (status === "loading" ? 999999 : status === "error" ? 8_000 : 800)
          )
        );
      }
    };
    toastTarget.addEventListener("toast", listener);
    return () => {
      toastTarget.removeEventListener("toast", listener);
    };
  }, [hideTimeout]);

  return (
    <div className={`toast ${visible ? "visible" : "hidden"} ${status ? "status-" + status.toLowerCase() : ""}`}>
      {status && <StatusIcon status={status} />}
      {content}
    </div>
  );
}

type StatusIconProps = {
  status: Status;
};

function StatusIcon({ status }: StatusIconProps) {
  switch (status) {
    case "ok":
      return <CheckCircle className="icon-green" />;
    case "loading":
      return <Ellipsis className="icon-blue" />;
    case "error":
      return <XCircle className="icon-red" />;
    default:
      return null;
  }
}
