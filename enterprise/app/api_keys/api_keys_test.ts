import { api_key } from "../../../proto/api_key_ts_proto";
import { ApiKeyField } from "./api_keys";

describe("ApiKeyField", () => {
  it("resets the cached API key value when the key changes", () => {
    const firstKey = new api_key.ApiKey({ id: "key-1", value: "AKFIRST" });
    const secondKey = new api_key.ApiKey({ id: "key-2", value: "AKSECOND" });
    const field = new ApiKeyField({ apiKey: firstKey });
    (field as any).setState = (state: any, callback?: () => void) => {
      (field as any).state = { ...field.state, ...state };
      callback?.();
    };

    field.componentDidMount();
    expect((field as any).value).toBe("AKFIRST");
    (field as any).state = {
      ...field.state,
      hideValue: false,
      displayValue: "AKFIRST",
    };

    (field as any).props = { apiKey: secondKey };
    field.componentDidUpdate({ apiKey: firstKey });

    expect((field as any).value).toBe("AKSECOND");
    expect(field.state.hideValue).toBe(true);
    expect(field.state.displayValue).toBe("••••••••••••••••••••");
  });
});
