export function isInSubtreeOf(element: Element, target: Element) {
  let current: Element | null = target;
  while (current) {
    if (current === element) return true;
    current = current.parentElement;
  }
  return false;
}

export function setClassEnabled(element: HTMLElement, className: string, enabled: boolean) {
  if (enabled) {
    element.classList.add(className);
  } else {
    element.classList.remove(className);
  }
}

export function setOrRemoveStyleProperty(
  element: HTMLElement,
  property: string,
  value: string,
  { important = false } = {}
) {
  if (value) {
    element.style.setProperty(property, value, important ? "important" : undefined);
  } else {
    element.style.removeProperty(property);
  }
}

export function createSvgElement(name: string) {
  return document.createElementNS("http://www.w3.org/2000/svg", name);
}
