import React from "react";
import { directReactChildren } from "./react";

describe("directReactChildren", () => {
  it("should treat fragment children as direct children", () => {
    const children = directReactChildren([
      <span key="first">first</span>,
      <>
        <span>second</span>
        {false}
        {null}
        <>{3}</>
      </>,
      <span>{"fourth"}</span>,
      undefined,
      "fifth",
    ]);

    expect(children.length).toBe(5);
    expect((children[0] as React.ReactElement).props.children).toBe("first");
    expect((children[1] as React.ReactElement).props.children).toBe("second");
    expect(children[2]).toBe(3);
    expect((children[3] as React.ReactElement).props.children).toBe("fourth");
    expect(children[4]).toBe("fifth");
  });
});
