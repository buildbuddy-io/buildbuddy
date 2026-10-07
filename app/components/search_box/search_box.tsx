import React from "react";
import TextInput from "../input/input";
import { Completer, Completion, Token, apply, ghostFor, tokenAt } from "./completion";

export type SearchBoxProps = {
  value: string;
  /** Callback to propagate changes to the query either by the user or by completion. */
  onChange: (value: string) => void;
  /** The passed completer is responsible for taking the token and providing completion options. */
  complete: Completer;
  /** Keys the box does not use itself, such as result navigation, are passed on. */
  onKeyDown?: (e: React.KeyboardEvent<HTMLInputElement>) => void;
  placeholder?: string;
  className?: string;
  autoFocus?: boolean;
  inputRef: React.RefObject<HTMLInputElement>;
};

interface State {
  token?: Token;
  /** The part of the token before the caret, which is what gets completed. */
  typed: string;
  /** What the completions answer; they only apply while that is being extended. */
  fetchedFor?: { token: Token; typed: string };
  /**
   * Whether the grey completion may be drawn: the caret was at the end when
   * the token was read, and the overlay still lines up with the input.
   */
  showGhost: boolean;
  completions: Completion[];
  /** Highlighted index in the auto-complete popup. -1 if nothing selected. */
  selected: number;
  open: boolean;
}

const COMPLETE_DEBOUNCE_MS = 50;

/**
 * A text input that supports auto-complete. As the user-types text, the
 * caller provided completer is invoked to get completion options. The
 * first completion is displayed as grey ghost text and the rest are shown in a popup.
 */
export default class SearchBox extends React.Component<SearchBoxProps, State> {
  state: State = { typed: "", showGhost: false, completions: [], selected: -1, open: false };
  private timer?: number;
  private latest = 0;
  private last = { value: "", caret: -1 };
  private list = React.createRef<HTMLUListElement>();

  componentDidUpdate(_: SearchBoxProps, prev: State) {
    // Make sure the selected row stays in view when scrolling with arrows.
    if (this.state.selected !== prev.selected && this.state.selected >= 0) {
      this.list.current?.querySelector(".selected")?.scrollIntoView({ block: "nearest" });
    }
  }

  componentWillUnmount() {
    window.clearTimeout(this.timer);
  }

  // Schedules the completer on a debounce timer to retrieve suggestions and display them to the user.
  private refresh(value: string, caret: number) {
    this.last = { value, caret };
    const { token, typed } = tokenAt(value, caret);
    // If the text is longer than the input box, stop showing the ghost text
    // since it won't line up anymore. We don't expect real queries to be this
    // long, and we can address it in the future if it actually turns out to
    // be a problem.
    const scrolled = (this.props.inputRef.current?.scrollLeft ?? 0) > 0;
    this.setState({ token, typed, showGhost: caret === value.length && !scrolled, selected: -1 });
    window.clearTimeout(this.timer);
    this.timer = window.setTimeout(() => {
      const seq = ++this.latest;
      this.props
        .complete(typed)
        .then((completions) => {
          // Ignore outdated results.
          if (seq !== this.latest) return;
          // Don't show popup for a single result.
          const trivial = completions.length === 1 && completions[0].text.toLowerCase() === typed.toLowerCase();
          this.setState({
            completions,
            fetchedFor: { token, typed },
            selected: -1,
            open: completions.length > 0 && !trivial,
          });
        })
        .catch(() => {
          // Ignore errors for now, user can still manually type the query.
        });
    }, COMPLETE_DEBOUNCE_MS);
  }

  /** Hides the auto-complete popup and clears completion state. */
  private dismiss() {
    window.clearTimeout(this.timer);
    this.latest++;
    this.setState({ completions: [], selected: -1, open: false });
  }

  /** Returns the completions that are meaningful for the current state. */
  private applicableCompletions(): Completion[] {
    const { token, fetchedFor, completions } = this.state;
    // Nothing if the fetched completions are not for the current token.
    if (!token || !fetchedFor || token.start !== fetchedFor.token.start) return [];
    const typed = this.state.typed.toLowerCase();
    if (!typed.startsWith(fetchedFor.typed.toLowerCase())) return [];
    // Filter the completions by the typed prefix.
    return completions.filter((c) => c.text.toLowerCase().startsWith(typed));
  }

  private accept(c: Completion) {
    const token = this.state.token;
    if (!token) return;
    const next = apply(this.props.value, token, c);
    this.props.onChange(next.value);
    this.dismiss();
    // Once the new value has rendered, place the caret after the insertion
    // and ask what comes next.
    window.requestAnimationFrame(() => {
      this.props.inputRef.current?.setSelectionRange(next.caret, next.caret);
      this.refresh(next.value, next.caret);
    });
  }

  private ghost(completions: Completion[]) {
    const { typed, showGhost } = this.state;
    return showGhost ? ghostFor(typed, completions) : undefined;
  }

  private onKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    const { selected } = this.state;
    const completions = this.applicableCompletions();
    const open = this.state.open && completions.length > 0;
    const ghost = this.ghost(completions);
    switch (e.key) {
      case "Tab":
      case "ArrowRight":
        if (ghost && e.currentTarget.selectionStart === this.props.value.length) {
          this.accept(ghost.completion);
          e.preventDefault();
          return;
        }
        break;
      case "ArrowDown":
      case "ArrowUp":
        if (open) {
          const delta = e.key === "ArrowDown" ? 1 : -1;
          this.setState({ selected: Math.min(completions.length - 1, Math.max(0, selected + delta)) });
          e.preventDefault();
          return;
        }
        break;
      case "Enter":
        if (open) {
          if (selected >= 0 && completions[selected]) {
            this.accept(completions[selected]);
          } else {
            this.dismiss();
          }
          e.preventDefault();
          return;
        }
        break;
      case "Escape":
        if (open || ghost) {
          this.dismiss();
          e.preventDefault();
          return;
        }
        break;
    }
    this.props.onKeyDown?.(e);
  };

  render() {
    const { value, placeholder, className, autoFocus, inputRef } = this.props;
    const { selected } = this.state;
    const completions = this.applicableCompletions();
    const open = this.state.open && completions.length > 0;
    const ghost = this.ghost(completions);
    return (
      <div className="search-box">
        <TextInput
          ref={inputRef}
          className={className ?? ""}
          value={value}
          placeholder={placeholder}
          autoFocus={autoFocus}
          autoComplete="off"
          spellCheck={false}
          onChange={(e) => {
            this.props.onChange(e.target.value);
            this.refresh(e.target.value, e.target.selectionStart ?? e.target.value.length);
          }}
          onKeyDown={this.onKeyDown}
          onFocus={(e) => this.refresh(e.target.value, e.target.selectionStart ?? e.target.value.length)}
          // The caret moving to another token changes what is being completed.
          onSelect={(e) => {
            const el = e.currentTarget;
            const caret = el.selectionStart ?? el.value.length;
            if (el.value !== this.last.value || caret !== this.last.caret) this.refresh(el.value, caret);
          }}
          onBlur={() => this.dismiss()}
        />
        {ghost && (
          <div className="search-box-ghost" aria-hidden>
            <span className="search-box-ghost-typed">{value}</span>
            <span className="search-box-ghost-rest">{ghost.rest}</span>
          </div>
        )}
        {open && (
          // Mouse down anywhere on the list, its scrollbar included, must not
          // take focus from the input, which would close the list.
          <ul className="search-box-list" role="listbox" ref={this.list} onMouseDown={(e) => e.preventDefault()}>
            {completions.map((c, i) => (
              <li
                key={c.text}
                className={`search-box-item ${i === selected ? "selected" : ""}`}
                role="option"
                aria-selected={i === selected}
                onMouseDown={() => this.accept(c)}>
                <span className="search-box-item-text">{c.text}</span>
                {c.detail && <span className="search-box-item-detail">{c.detail}</span>}
                {c.count !== undefined && c.count > 0 && (
                  <span className="search-box-item-count">{c.count.toLocaleString()}</span>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
    );
  }
}
