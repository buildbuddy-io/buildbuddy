import React from "react";
import FilledButton from "../../../app/components/button/button";
import TextInput from "../../../app/components/input/input";
import { TextLink } from "../../../app/components/link/link";
import Spinner from "../../../app/components/spinner/spinner";
import { secrets } from "../../../proto/secrets_ts_proto";
import rpc_service from "../../../app/service/rpc_service";
import error_service from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import alert_service from "../../../app/alert/alert_service";
import sodium from "libsodium-wrappers";

export interface UpdateSecretProps {
  name?: string;
}

interface State {
  name?: string;
  value?: string;

  loading?: boolean;
}

export default class UpdateSecretComponent extends React.Component<UpdateSecretProps, State> {
  state: State = {};

  private onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    this.setState({ loading: true });

    const name = this.props.name || this.state.name || "";
    const value = this.state.value || "";

    sodium.ready
      .then(() => {
        this.encryptAndUpdate(name!, value!);
      })
      .catch((e) => {
        error_service.handleError(e);
        this.setState({ loading: false });
      });
  }

  private encryptAndUpdate(name: string, value: string) {
    rpc_service.service
      .getPublicKey({})
      .then((response) => {
        const secret = this.encrypt(response.publicKey, name, value.trim());
        this.updateSecret(secret);
      })
      .catch((e) => {
        error_service.handleError(e);
        this.setState({ loading: false });
      });
  }

  private encrypt(publicKey: secrets.IPublicKey, name: string, value: string): secrets.ISecret {
    // See https://docs.github.com/en/rest/actions/secrets#example-encrypting-a-secret-using-nodejs
    const binkey = sodium.from_base64(publicKey.value, sodium.base64_variants.ORIGINAL);
    const binsec = sodium.from_string(value);
    const encBytes = sodium.crypto_box_seal(binsec, binkey);
    const output = sodium.to_base64(encBytes, sodium.base64_variants.ORIGINAL);

    return { name, value: output };
  }

  private updateSecret(secret: secrets.ISecret) {
    rpc_service.service
      .updateSecret({ secret })
      .then(() => {
        alert_service.success("Successfully encrypted and saved secret.");
        router.navigateTo("/settings/org/secrets");
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private onChangeSecretName(e: React.ChangeEvent<HTMLInputElement>) {
    const name = e.target.value;
    this.setState({ name });
  }

  private onChangeSecretValue(e: React.ChangeEvent<HTMLTextAreaElement>) {
    const value = e.target.value;
    this.setState({ value });
  }

  render() {
    return (
      <div className="update-secret">
        <div className="secrets-breadcrumbs">
          <div className="secrets-breadcrumb">
            <TextLink href="/settings/org/secrets">Secrets</TextLink>
          </div>
          <div className="secrets-breadcrumb">{this.props.name ? "Update secret" : "New secret"}</div>
        </div>
        <form className="secrets-form" autoComplete="off" noValidate onSubmit={this.onSubmit.bind(this)}>
          {this.props.name ? (
            <div className="secret-name code-font">{this.props.name}</div>
          ) : (
            <div className="form-field-group">
              <label htmlFor="secretName">Name</label>
              <div className="caption">
                Actions with secrets enabled will use this environment variable to get the secret's value.
                UPPERCASE_WITH_UNDERSCORES is recommended.
              </div>
              <div>
                <TextInput
                  name="secretName"
                  onChange={this.onChangeSecretName.bind(this)}
                  value={this.props.name || this.state.name}></TextInput>
              </div>
            </div>
          )}
          <div className="form-field-group">
            <label htmlFor="secretValue">Value</label>
            <textarea
              name="secretValue"
              className="text-input"
              onChange={this.onChangeSecretValue.bind(this)}></textarea>
          </div>
          <FilledButton disabled={this.state.loading} className="submit-button">
            <span>Save</span>
            {this.state.loading && <Spinner className="white" />}
          </FilledButton>
        </form>
      </div>
    );
  }
}
