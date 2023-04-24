import React from "react";
import Modal from "../../../app/components/modal/modal";
import shortcuts, { KeyCombo } from "../../../app/shortcuts/shortcuts";
import Dialog, { DialogBody, DialogFooter, DialogHeader, DialogTitle } from "../../../app/components/dialog/dialog";
import UserPreferences from "../../../app/preferences/preferences";

interface State {
  showing: boolean;
}

interface Props {
  preferences: UserPreferences;
}

export default class ShortcutsComponent extends React.Component<Props, State> {
  state: State = {
    showing: false,
  };

  componentWillMount() {
    shortcuts.register(KeyCombo.question, () => {
      this.setState({ showing: true });
    });
    shortcuts.register(KeyCombo.esc, () => {
      this.setState({ showing: false });
    });
  }

  render() {
    return (
      <Modal isOpen={this.state.showing}>
        <Dialog>
          <DialogHeader>
            <DialogTitle className="keyboard-shortcut-title">BuildBuddy Keyboard Shortcuts</DialogTitle>
          </DialogHeader>
          <DialogBody>
            <table className="keyboard-shortcut-help">
              <tr>
                <th className="keyboard-shortcut-th"></th>
                <th className="keyboard-shortcut-th">Navigation</th>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">j/k</td>
                <td>Select previous / next item (vertical)</td>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">Enter</td>
                <td>Open selected item</td>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">u</td>
                <td>Go back</td>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">g-a</td>
                <td>Go to All Builds page</td>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">g-r</td>
                <td>Go to Trends page</td>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">g-t</td>
                <td>Go to Tests page</td>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">g-x</td>
                <td>Go to Executors page</td>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">g-q</td>
                <td>Go to Quickstart page</td>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">g-g</td>
                <td>Go to Settings page</td>
              </tr>
              <tr>
                <th className="keyboard-shortcut-th">&nbsp;</th>
                <th className="keyboard-shortcut-th"></th>
              </tr>
              <tr>
                <th className="keyboard-shortcut-th"></th>
                <th className="keyboard-shortcut-th">Invocations</th>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">Shift + c</td>
                <td>Copy invocation link</td>
              </tr>
              <tr>
                <th className="keyboard-shortcut-th">&nbsp;</th>
                <th className="keyboard-shortcut-th"></th>
              </tr>
              <tr>
                <th className="keyboard-shortcut-th"></th>
                <th className="keyboard-shortcut-th">Help</th>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">?</td>
                <td>Open keyboard shortcuts help</td>
              </tr>
              <tr>
                <td className="keyboard-shortcut-key">Esc</td>
                <td>Close keyboard shortcuts help</td>
              </tr>
            </table>
          </DialogBody>
          <DialogFooter>
            <div
              className="keyboard-shortcut-close"
              onClick={() => {
                this.setState({ showing: false });
              }}>
              Close
            </div>
          </DialogFooter>
        </Dialog>
      </Modal>
    );
  }
}
