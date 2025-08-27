import React from "react";
import Checkbox from "../../../app/components/checkbox/checkbox";
import Select, { Option } from "../../../app/components/select/select";
import { executor } from "../../../proto/executor_ts_proto";
import rpc_service from "../../../app/service/rpc_service";

// TODO(iain): select appropriate default values.
interface Props {
  visible: boolean;
  pool: string;
  configFormData: {
    operatingSystem: string;
    xcodeVersions: string[];
    xcodeSDKs: string[];
  };
  onClose: () => void;
  onFormChange: (field: keyof Props['configFormData'], value: string | string[]) => void;
}

interface State {
  loading: boolean;
  operatingSystemVersions: executor.OperatingSystemVersion[];
  xcodeVersions: executor.XcodeVersion[];
  xcodeSDKs: executor.XcodeSDK[];
  error: string | null;
}

export default class ExecutorConfigModal extends React.Component<Props, State> {
  state: State = {
    operatingSystemVersions: [],
    xcodeVersions: [],
    xcodeSDKs: [],
    loading: false,
    error: null,
  };

  componentDidMount() {
    this.setState({ loading: true });
    rpc_service.service.getExecutorConfigurations({})
      .then((response) => {
        this.setState({
          operatingSystemVersions: response.operatingSystem || [],
          xcodeVersions: response.xcode || [],
          xcodeSDKs: response.xcodeSdk || [],
          loading: false
        });
      })
      .catch((error) => {
        this.setState({
          error: error.message || 'Failed to load executor configurations',
          loading: false
        });
      });
  }

  onXcodeVersionChange = (version: string, checked: boolean) => {
    const currentVersions = this.props.configFormData.xcodeVersions || [];
    const newVersions = checked
      ? [...currentVersions, version]
      : currentVersions.filter(v => v !== version);
    this.props.onFormChange('xcodeVersions', newVersions);
  };

  onXcodeSDKChange = (sdk: string, checked: boolean) => {
    const currentSDKs = this.props.configFormData.xcodeSDKs || [];
    const newSDKs = checked
      ? [...currentSDKs, sdk]
      : currentSDKs.filter(s => s !== sdk);
    this.props.onFormChange('xcodeSDKs', newSDKs);
  };

  onApply = () => {
    const { configFormData } = this.props;
    
    // Find the selected operating system version object
    const selectedOS = this.state.operatingSystemVersions.find(
      os => os.version === configFormData.operatingSystem
    );

    // Find the selected Xcode version objects
    const selectedXcodeVersions = this.state.xcodeVersions.filter(
      xcode => configFormData.xcodeVersions?.includes(xcode.version || '')
    );

    // Find the selected Xcode SDK objects
    const selectedXcodeSDKs = this.state.xcodeSDKs.filter(
      sdk => configFormData.xcodeSDKs?.includes(`${sdk.family}-${sdk.version}`)
    );

    const request: executor.ApplyConfigurationRequest = {
      pool: this.props.pool,
      operatingSystem: selectedOS,
      xcode: selectedXcodeVersions,
      xcodeSDK: selectedXcodeSDKs,
    };

    this.setState({ loading: true, error: null });
    
    rpc_service.service.applyExecutorConfiguration(request)
      .then(() => {
        this.setState({ loading: false });
        this.props.onClose();
      })
      .catch((error) => {
        this.setState({
          error: error.message || 'Failed to apply executor pool configuration/',
          loading: false
        });
      });
  };

  render() {
    if (!this.props.visible) return null;

    return (
      <div className="config-modal-overlay" onClick={this.props.onClose}>
        <div className="config-modal" onClick={(e) => e.stopPropagation()}>
          <div className="config-modal-header">
            <h2>Configure Executor Pool</h2>
          </div>
          <div className="config-modal-body">
            <div className="config-form-row">
              <label>Operating System:</label>
              {this.state.loading ? (
                <div>Loading...</div>
              ) : (
                <Select
                  value={this.props.configFormData.operatingSystem}
                  onChange={(e) => this.props.onFormChange('operatingSystem', e.target.value)}
                >
                  <Option key="" value="">
                    Select an operating system...
                  </Option>
                  {this.state.operatingSystemVersions.map((os) => (
                    <Option key={os.version} value={os.version || ''}>
                      {os.displayName || os.version}
                    </Option>
                  ))}
                </Select>
              )}
            </div>
            <div className="config-form-row">
              <label>Xcode Version(s):</label>
              {this.state.loading ? (
                <div>Loading...</div>
              ) : (
                <div className="option-group">
                  <div className="option-group-options">
                    {this.state.xcodeVersions.map((xcode) => (
                      <div key={xcode.version}>
                        <label>
                          <Checkbox 
                            checked={this.props.configFormData.xcodeVersions?.includes(xcode.version || '') || false}
                            onChange={(e) => this.onXcodeVersionChange(xcode.version || '', e.target.checked)}
                          />
                          <span className="xcode-label">{xcode.displayName || xcode.version}</span>
                        </label>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
            <div className="config-form-row">
              <label>Xcode Simulator Runtime(s):</label>
              {this.state.loading ? (
                <div>Loading...</div>
              ) : (
                <div className="option-group">
                  <div className="option-group-options">
                    {this.state.xcodeSDKs.map((sdk) => (
                      <div key={`${sdk.family}-${sdk.version}`}>
                        <label>
                          <Checkbox 
                            checked={this.props.configFormData.xcodeSDKs?.includes(`${sdk.family}-${sdk.version}`) || false}
                            onChange={(e) => this.onXcodeSDKChange(`${sdk.family}-${sdk.version}`, e.target.checked)}
                          />
                          <span className="xcode-label">{sdk.displayName || `${sdk.family} ${sdk.version}`}</span>
                        </label>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
          {this.state.error && (
            <div className="config-modal-error">
              <div className="error">{this.state.error}</div>
            </div>
          )}
          <div className="config-modal-support">
            Can't find the version you're looking for? Contact BuildBuddy support for help.
          </div>
          <div className="config-modal-footer">
            <button className="cancel-button" onClick={this.props.onClose}>
              Cancel
            </button>
            <button className="apply-button" onClick={this.onApply}>
              Apply
            </button>
          </div>
        </div>
      </div>
    );
  }
}
