import { Bot } from "lucide-react";
import React from "react";

export interface Props {
  suggestions: string[];
}

export function InvocationBotCard({ suggestions }: Props): JSX.Element {
  return (
    <>
      {suggestions.map((suggestion) => (
        <div className="card card-suggestion card-suggestion-bot">
          <Bot className="icon" />
          <div className="content">
            <div className="details">
              <div className="card-suggestion-message">{suggestion.trim()}</div>
              <div className="card-suggestion-reason">Based on the error message in your build logs.</div>
            </div>
          </div>
        </div>
      ))}
    </>
  );
}
