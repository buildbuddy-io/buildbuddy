import React from "react";
import alert_service from "../../../app/alert/alert_service";
import FilledButton from "../../../app/components/button/button";
import TextInput, { TextInputProps } from "../../../app/components/input/input";
import { quota } from "../../../proto/quota_ts_proto";

export interface BucketFormValues {
  namespace?: string;
  bucket?: quota.IBucket;
}

export interface BucketFormProps {
  initialValues?: {
    namespace?: string;
    bucket?: quota.IBucket;
  };
  namespaceEditable?: boolean;
  nameEditable?: boolean;
  onSubmit: (values: BucketFormValues) => Promise<any>;
  submitLabel: string;
}

interface State {
  loading?: boolean;
  values: Record<string, string>;
  invalid: Record<string, boolean>;
}

type FieldProps = TextInputProps & {
  initialValue?: string | number;
};

export default class BucketForm extends React.Component<BucketFormProps, State> {
  state: State = {
    values: {},
    invalid: {},
  };

  private onChange(e: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ values: { ...this.state.values, [e.target.name]: e.target.value } });
  }
  private onInput(e: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ invalid: { ...this.state.invalid, [(e.target as HTMLInputElement).name]: false } });
  }
  private onBlur(e: React.FocusEvent<HTMLInputElement>) {
    e.target.checkValidity();
  }
  private onInvalid(e: React.FormEvent<HTMLInputElement>) {
    this.setState({ invalid: { ...this.state.invalid, [(e.target as HTMLInputElement).name]: true } });
  }

  private onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    if (!(e.target as HTMLFormElement).checkValidity()) {
      alert_service.error("Fix invalid inputs and try again.");
      return;
    }
    const values: BucketFormValues = {
      namespace: this.state.values["namespace"] || this.props.initialValues?.namespace,
      bucket: {
        name: this.state.values["bucket.name"] || this.props.initialValues?.bucket?.name,
        maxRate: {
          numRequests: (Number(this.state.values["bucket.maxRate.numRequests"]) ||
            this.props.initialValues?.bucket?.maxRate?.numRequests) as any,
          period: {
            seconds: (Number(this.state.values["bucket.maxRate.period.seconds"]) ||
              this.props.initialValues?.bucket?.maxRate?.period?.seconds) as any,
          },
        },
        maxBurst: (Number(this.state.values["bucket.maxBurst"]) || this.props.initialValues?.bucket?.maxBurst) as any,
      },
    };
    this.setState({ loading: true });
    this.props.onSubmit(values).finally(() => this.setState({ loading: false }));
  }

  private Field = ({ name = "", placeholder = "", initialValue, ...rest }: FieldProps) => {
    return (
      <TextInput
        placeholder={placeholder}
        name={name}
        value={name in this.state.values ? this.state.values[name] : initialValue}
        onInput={this.onInput.bind(this)}
        onChange={this.onChange.bind(this)}
        onBlur={this.onBlur.bind(this)}
        onInvalid={this.onInvalid.bind(this)}
        className={this.state.invalid[name] ? "invalid" : ""}
        required
        {...rest}
      />
    );
  };

  render() {
    return (
      <form autoComplete="off" onSubmit={this.onSubmit.bind(this)} className="bucket-form" noValidate>
        {this.props.namespaceEditable && (
          <div className="labeled-input">
            <label>Namespace name (required)</label>
            <this.Field name="namespace" initialValue={this.props.initialValues?.namespace} placeholder="/rpc/FooBar" />
            <div className="validation-message">Required.</div>
          </div>
        )}
        {this.props.nameEditable && (
          <div className="labeled-input">
            <label>Bucket name (enter "default" for the default bucket)</label>
            <this.Field
              name="bucket.name"
              initialValue={this.props.initialValues?.bucket?.name}
              placeholder="free-tier"
            />
            <div className="validation-message">Required.</div>
          </div>
        )}
        <div className="labeled-input">
          <label>Bucket max rate: number of requests per time period</label>
          <this.Field
            name="bucket.maxRate.numRequests"
            initialValue={Number(this.props.initialValues?.bucket?.maxRate?.numRequests)}
            placeholder="100"
            type="number"
            min="0"
          />
          <div className="validation-message">Enter a positive integer.</div>
        </div>
        <div className="labeled-input">
          <label>Bucket max rate: time period (seconds)</label>
          <this.Field
            name="bucket.maxRate.period.seconds"
            initialValue={Number(this.props.initialValues?.bucket?.maxRate?.period?.seconds)}
            placeholder="60"
            type="number"
            min="0"
          />
          <div className="validation-message">Enter a positive integer.</div>
        </div>
        <div className="labeled-input">
          <label>Bucket max burst</label>
          <this.Field
            name="bucket.maxBurst"
            initialValue={Number(this.props.initialValues?.bucket?.maxBurst)}
            placeholder="100"
            type="number"
            min="0"
          />
          <div className="validation-message">Enter a positive integer.</div>
        </div>
        <div>
          <FilledButton disabled={this.state.loading} className="submit-button">
            {this.props.submitLabel}
          </FilledButton>
        </div>
      </form>
    );
  }
}
