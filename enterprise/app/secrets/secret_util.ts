import rpc_service from "../../../app/service/rpc_service";
import { secrets } from "../../../proto/secrets_ts_proto";
import sodium from "libsodium-wrappers";

export function encryptAndUpdate(name: string, value: string) {
  return updateSecret(new secrets.Secret({
    name: name,
    value: value,
  }));
}

function encrypt(publicKey: secrets.PublicKey, name: string, value: string): secrets.ISecret {
  // See https://docs.github.com/en/rest/actions/secrets#example-encrypting-a-secret-using-nodejs
  const binkey = sodium.from_base64(publicKey.value, sodium.base64_variants.ORIGINAL);
  const binsec = sodium.from_string(value);
  const encBytes = sodium.crypto_box_seal(binsec, binkey);
  const output = sodium.to_base64(encBytes, sodium.base64_variants.ORIGINAL);
  return { name, value: output };
}

function updateSecret(secret: secrets.ISecret) {
  return rpc_service.service.updateSecret(
    secrets.UpdateSecretRequest.create({ secret: secrets.Secret.create(secret) })
  );
}
