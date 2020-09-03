import { EventPublisher, EventListener } from "./events";

export default class Disposer {
  private readonly subscriptions: any[] = [];
  private readonly animations: (() => void)[] = [];
  private readonly disposables: Disposable[] = [];

  subscribe(object: EventPublisher, eventName: string, listener: EventListener) {
    object.addEventListener(eventName, listener);
    this.subscriptions.push([object, eventName, listener]);

    return this;
  }

  runAnimationLoop(fn: () => void) {
    let cancelled = false;
    const loop = () => {
      if (cancelled) return;
      fn();
      requestAnimationFrame(loop);
    };
    this.animations.push(() => {
      cancelled = true;
    });
    loop();

    return this;
  }

  add(disposable: Disposable) {
    this.disposables.push(disposable);

    return this;
  }

  dispose() {
    for (const [el, eventName, listener] of this.subscriptions) {
      el.removeEventListener(eventName, listener);
    }
    for (const cancelFn of this.animations) {
      cancelFn();
    }
    for (const disposable of this.disposables) {
      disposable.dispose();
    }
  }
}

export interface Disposable {
  dispose: () => void;
}
