import React from "react";

/**
 * Returns a flat list of direct children, treating fragment children as
 * though they were passed directly.
 *
 * For example:
 *
 * ```tsx
 * directReactChildren(
 *   <>
 *     <span>Foo</span>
 *     <>
 *       <span>Bar</span>
 *     </>
 *   </>
 * )
 * ```
 *
 * returns:
 *
 * ```tsx
 * [ <span>Foo</span>, <span>Bar</span> ]
 * ```
 *
 * Arrays and falsy values are normalized the same way as
 * `React.Children.toArray`.
 */
export function directReactChildren(children: React.ReactNode): React.ReactNode[] {
  const direct: React.ReactNode[] = [];
  pushAllDirectChildren(children, direct);
  return direct;
}

function pushAllDirectChildren(children: React.ReactNode, direct: React.ReactNode[]): void {
  for (const child of React.Children.toArray(children)) {
    if (React.isValidElement<{ children?: React.ReactNode }>(child) && child.type === React.Fragment) {
      pushAllDirectChildren(child.props.children, direct);
      continue;
    }
    direct.push(child);
  }
}

/**
 * Joins a list of react nodes with the given separator. Similar to
 * String.prototype.join, but for JSX.
 *
 * Example:
 * ```tsx
 * joinReactNodes([<MyComponent />, <MyComponent />], ", ")
 * ```
 */
export function joinReactNodes(nodes: React.ReactNode[], joiner: React.ReactNode): React.ReactNode[] {
  const joined: React.ReactNode[] = [];
  for (let i = 0; i < nodes.length; i++) {
    joined.push(nodes[i]);
    // If the next element exists, append the joiner node.
    if (i + 1 < nodes.length) {
      joined.push(joiner);
    }
  }
  return joined;
}

/**
 * Wraps a component with a derived `key` prop, causing the wrapped component
 * instance to be discarded and re-rendered if the value returned by `getKey`
 * changes.
 *
 * This is useful for simplifying state management, so that the rendered
 * component doesn't have to manually invalidate any cached values and
 * re-initialize state.
 *
 * Example:
 *
 * ```tsx
 * class FooInner extends React.Component<FooProps> {}
 * // Wrapper around `FooInner` that unmounts and remounts a new `Foo`
 * // if `props.foo.id` changes
 * class Foo extends withKey(FooInner, (props) => props.foo.id);
 * ```
 */
export function withKey<P>(
  Component: React.ComponentType<P>,
  getKey: (props: P) => React.Key
): React.ComponentClass<P> {
  return class extends React.Component<P> {
    render() {
      return <Component key={getKey(this.props)} {...this.props} />;
    }
  };
}
