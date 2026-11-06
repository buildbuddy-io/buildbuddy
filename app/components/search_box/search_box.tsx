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
// A completer may answer with hundreds of rows; only this many are shown.
// They come most relevant first, and the grey text only needs the first.
const SHOWN_LIMIT = 50;

/**
 * A text input that supports auto-complete. As the user-types text, the
 * caller provided completer is invoked to get completion options. The
 * first completion is displayed as grey ghost text and the rest are shown in a popup.
 */
export default class SearchBox extends React.Component<SearchBoxProps, State> {
  state: State = { typed: "", showGhost: false, completions: [], selected: -1, open: false };
  private timer?: number;
  private frame?: number;
  // Each completion request gets an increasing sequence number.
  // If the sequence doesn't match by the time we get the completion
  // callback then we know the response is stale.
  private latestCompletionSeq = 0;
  // The search string and caret position at the time the last completion refresh was invoked.
  private refreshedFor = { value: "", caret: -1 };
  private list = React.createRef<HTMLUListElement>();

  componentDidUpdate(prevProps: SearchBoxProps, prev: State) {
    // Reset if the search string changed through something other than the search box
    // (e.g. navigation).
    if (this.props.value !== prevProps.value && this.props.value !== this.refreshedFor.value) {
      this.dismiss();
    }
    // Make sure the selected row stays in view when scrolling with arrows.
    if (this.state.selected !== prev.selected && this.state.selected >= 0) {
      this.list.current?.querySelector(".selected")?.scrollIntoView({ block: "nearest" });
    }
  }

  componentWillUnmount() {
    this.cancelPending();
  }

  /** Stops everything scheduled: the debounce, the frame after an accept, and any answer on its way. */
  private cancelPending() {
    window.clearTimeout(this.timer);
    if (this.frame !== undefined) window.cancelAnimationFrame(this.frame);
    this.frame = undefined;
    this.latestCompletionSeq++;
  }

  /** Where the caret is, or nothing while text is selected, which completion must not replace. */
  private caretOf(el: HTMLInputElement): number | undefined {
    if (el.selectionStart !== el.selectionEnd) return undefined;
    return el.selectionStart ?? el.value.length;
  }

  /** Reads the caret and refreshes, or dismisses when there is nothing to complete there. */
  private refreshFrom(el: HTMLInputElement) {
    const caret = this.caretOf(el);
    if (caret === undefined) {
      this.dismiss();
      return;
    }
    if (el.value !== this.refreshedFor.value || caret !== this.refreshedFor.caret) this.refresh(el.value, caret);
  }

  // Schedules the completer on a debounce timer to retrieve suggestions and display them to the user.
  private refresh(value: string, caret: number) {
    this.refreshedFor = { value, caret };
    const { token, typed } = tokenAt(value, caret);
    // Only the end of a token is completed; replacing a token someone has
    // clicked into the middle of would throw away what follows the caret.
    if (caret !== token.end) {
      this.setState({ token, typed, showGhost: false });
      this.dismiss();
      return;
    }
    // If the text is longer than the input box, stop showing the ghost text
    // since it won't line up anymore. We don't expect real queries to be this
    // long, and we can address it in the future if it actually turns out to
    // be a problem.
    const scrolled = (this.props.inputRef.current?.scrollLeft ?? 0) > 0;
    this.setState({ token, typed, showGhost: caret === value.length && !scrolled, selected: -1 });
    window.clearTimeout(this.timer);
    this.timer = window.setTimeout(() => {
      const seq = ++this.latestCompletionSeq;
      this.props
        .complete(typed)
        .then((completions) => {
          // Ignore outdated results.
          if (seq !== this.latestCompletionSeq) return;
          // Don't show popup for a single result.
          const trivial = completions.length === 1 && completions[0].text.toLowerCase() === typed.toLowerCase();
          // A row picked while the previous answer was showing stays picked
          // if the new answer still has it.
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
    this.cancelPending();
    // Forgotten too, so the next focus or caret event asks afresh even at
    // the same spot.
    this.refreshedFor = { value: "", caret: -1 };
    this.setState({ completions: [], selected: -1, open: false });
  }

  /** Returns the completions that are meaningful for the current state. */
  private applicableCompletions(): Completion[] {
    const { token, typed, fetchedFor, completions } = this.state;
    return applicable(token, typed, fetchedFor, completions);
  }

  private accept(c: Completion) {
    const token = this.state.token;
    if (!token) return;
    const next = apply(this.props.value, token, c);
    this.props.onChange(next.value);
    this.dismiss();
    // Once the new value has rendered, place the caret after the insertion
    // and ask what comes next.
    this.frame = window.requestAnimationFrame(() => {
      this.frame = undefined;
      this.props.inputRef.current?.setSelectionRange(next.caret, next.caret);
      this.refresh(next.value, next.caret);
    });
  }

  /** The grey completion: the highlighted row when it extends what was typed, else the first that does. */
  private ghost(completions: Completion[]) {
    const { typed, showGhost, selected } = this.state;
    if (!showGhost) return undefined;
    const picked = completions[selected];
    return (picked && ghostFor(typed, [picked])) || ghostFor(typed, completions);
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
          // Up from the first row is back to nothing highlighted, where Enter
          // puts the list away rather than taking a row.
          this.setState({ selected: Math.min(completions.length - 1, Math.max(-1, selected + delta)) });
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
            this.refreshFrom(e.target);
          }}
          onKeyDown={this.onKeyDown}
          onFocus={(e) => this.refreshFrom(e.target)}
          // The caret moving to another token changes what is being completed.
          onSelect={(e) => this.refreshFrom(e.currentTarget)}
          onBlur={() => this.dismiss()}
        />
        {ghost && (
          // Styled as the input itself, so it keeps the input's box however
          // that is styled, with the typed part invisible.
          <div className={`text-input search-box-ghost ${className ?? ""}`} aria-hidden>
            <span className="search-box-ghost-typed">{value}</span>
            <span className="search-box-ghost-rest">{ghost.rest}</span>
          </div>
        )}
        {open && (
          // Mouse down anywhere on the list, its scrollbar included, must not
          // take focus from the input, which would close the list.
          <ul className="search-box-list" ref={this.list} onMouseDown={(e) => e.preventDefault()}>
            {completions.map((c, i) => (
              <li
                key={c.text}
                className={`search-box-item ${i === selected ? "selected" : ""}`}
                onMouseDown={(e) => {
                  if (e.button === 0) this.accept(c);
                }}>
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

/**
 * The completions that apply to what is typed now: those fetched for this
 * very token, as long as it has only been extended since, narrowed to the
 * ones starting with the typed text.
 */
function applicable(
  token: Token | undefined,
  typed: string,
  fetchedFor: { token: Token; typed: string } | undefined,
  completions: Completion[]
): Completion[] {
  if (!token || !fetchedFor || token.start !== fetchedFor.token.start) return [];
  const lower = typed.toLowerCase();
  if (!lower.startsWith(fetchedFor.typed.toLowerCase())) return [];
  return completions.filter((c) => c.text.toLowerCase().startsWith(lower)).slice(0, SHOWN_LIMIT);
}
