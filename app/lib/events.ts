import React from "react";

export type EventListener<T = any> = (e: T) => void;

export interface EventPublisher<T extends string = string> {
  addEventListener: (name: T, listener: EventListener<T>) => void;
  removeEventListener: (name: T, listener: EventListener<T>) => void;
}

/** Interface supporting event dispatch. */
export class EventNotifier<EventName extends string = string, EventData = any>
  implements EventNotifierInterface<EventName, EventData> {
  private listeners = {} as Record<EventName, EventListener<EventData>[]>;

  static fromOnOffEmitConvention<EventName extends string = string, EventData = any>(
    object: any
  ): EventNotifierInterface<EventName, EventData> {
    return {
      dispatch(name: EventName, data: EventData) {
        object.emit(name, data);
      },
      addEventListener(name: EventName, listener: EventListener<EventData>) {
        object.on(name, listener);
      },
      removeEventListener(name: EventName, listener: EventListener<EventData>) {
        object.off(name, listener);
      },
    };
  }

  dispatch(name: EventName, data: EventData) {
    for (const listener of this.listeners[name] || []) {
      listener(data);
    }
  }

  addEventListener(name: EventName, listener: EventListener<EventData>) {
    if (!this.listeners[name]) this.listeners[name] = [];
    this.listeners[name].push(listener);
  }

  removeEventListener(name: EventName, listener: EventListener<EventData>) {
    if (this.listeners[name]) {
      this.listeners[name] = this.listeners[name].filter(
        (registeredListener) => registeredListener !== listener
      );
      if (!this.listeners[name].length) {
        delete this.listeners[name];
      }
    }
  }
}

export type EventNotifierInterface<EventName extends string = string, EventData = any> = Omit<
  EventNotifier<EventName, EventData>,
  "listeners"
>;

export function useEventListener<EventName extends string = string>(
  object: EventNotifierInterface<EventName> | EventTarget | null | undefined,
  eventName: EventName,
  listener: EventListener
) {
  React.useEffect(() => {
    if (!object || !eventName || !listener) return;
    object.addEventListener(eventName, listener);
    return () => object.removeEventListener(eventName, listener);
  }, [object, eventName, listener]);
}
