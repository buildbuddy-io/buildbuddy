import React from "react";
import TerminalComponent from "../terminal/terminal";

interface Props {
  testCases: Element[];
  tagName?: string;
  dark: boolean;
}

export default class TargetTestSuiteComponent extends React.Component<Props> {
  render() {
    return (
      <div className="test-document">
        <div className="test-suite">
          <div className="test-cases">
            {this.props.testCases.map((testCase) => (
              <div className="test-case-container">
                <div className="test-case">
                  <div className="test-case-name">
                    {testCase.getAttribute("classname") && (
                      <span className="test-class">{testCase.getAttribute("classname")}.</span>
                    )}
                    {testCase.getAttribute("name")}
                  </div>
                  <div className="test-case-time">{testCase.getAttribute("time")} s</div>
                </div>
                {Array.from(testCase.children).map((child) => (
                  <div className="test-case-info">
                    <div className="test-case-message">
                      {child.getAttribute("message")} {child.getAttribute("type")}
                    </div>
                    {!!child.textContent?.trim() && (
                      <TerminalComponent
                        value={child.textContent
                          .replaceAll(`�[`, `\u001b[`)
                          .replaceAll(`#x1b[`, `\u001b[`)
                          .replaceAll(`#x1B[`, `\u001b[`)}
                        lightTheme={!this.props.dark}
                      />
                    )}
                  </div>
                ))}
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }
}
