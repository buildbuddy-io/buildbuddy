---
slug: customer-managed-encryption-keys
title: Providing Control Over Cache Encryption
description: We are announcing a feature that allows customers to control how their data is encrypted in in our cache.
author: Vadim Berezniker
author_title: Engineer @ BuildBuddy
date: 2023-06-05:11:00:00
author_url: https://www.linkedin.com/in/vadimberezniker/
author_image_url: https://avatars.githubusercontent.com/u/1335358?v=4
image: /img/blog/cmek.png
tags: [product, security, encryption, enterprise]
---

BuildBuddy enables fast builds by providing a high-performance cache that stores and serves artifacts, such as the
inputs and outputs to your build actions.

Starting today, BuildBuddy customers can provide their own encryption keys that will be used to encrypt and decrypt data
stored in the cache. At launch, we are supporting keys managed by [Google Cloud Platform KMS](https://cloud.google.com/security-key-management) and [Amazon Web Services KMS](https://aws.amazon.com/kms/).

<!-- truncate -->

To get started, head over to your organization Settings page and look for the "Encryption Keys" tab. Note that these
settings are only visible to Organization Administrators.

For more details, see the [Customer Managed Encryption Keys documentation](https://www.buildbuddy.io/docs/cache-encryption-keys).

## Encryption model

We’ve modeled our Customer Managed Encryption Key implementation on Snowflake’s [Tri-Secret Secure](https://docs.snowflake.com/en/user-guide/security-encryption-manage#tri-secret-secure) design.

In this model, the customer-supplied key is combined with a BuildBuddy-maintained key to create a composite master key
that is used to protect your BuildBuddy data.

If the customer-managed key in the composite master key is revoked, your data can no longer be decrypted by BuildBuddy -
providing a level of security and control above BuildBuddy’s standard level of encryption that is controlled by GCP.

This dual-key encryption model, together with BuildBuddy’s built-in user authentication enables three levels of data
protection.

## Technical Details

In order to generate the master key, two internal 256-bit symmetric keys are first created. These keys are then
encrypted by both the customer-supplied key and the BuildBuddy-managed key.

To perform cryptographic operations on data, these two keys are decrypted and passed through the
[HKDF-Expand](https://en.wikipedia.org/wiki/HKDF) key derivation function to generate a single master key. When cache
reads and writes are performed, the [XChaCha20-Poly1305](https://en.wikipedia.org/wiki/ChaCha20-Poly1305) algorithm is
used to encrypt, decrypt and authenticate the data using this master key.

Revocation of either the customer-supplied key or the BuildBuddy-managed key renders previously written artifacts undecryptable.

Both the encryption design and source code have been audited by a third party.

If you have any questions or feedback about this feature, please reach out to us on [Slack](https://slack.buildbuddy.io/)
or at [security@buildbuddy.io](mailto:security@buildbuddy.io).
