import React from "react";

export type EventListener<T = any> = (e: T) => void;

export function useEventListener<EventName extends string = string>(
  target: EventTarget | null | undefined,
  eventName: EventName,
  listener: EventListener
) {
  React.useEffect(() => {
    if (!target || !eventName || !listener) return;
    target.addEventListener(eventName, listener);
    return () => target.removeEventListener(eventName, listener);
  }, [target, eventName, listener]);
}
