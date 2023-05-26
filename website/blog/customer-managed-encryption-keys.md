---
slug: customer-managed-encryption-keys
title: Providing Control Over Cache Encryption
description: We are announcing a feature that allows customers to control how their data is encrypted in in our cache.
author: Vadim Berezniker
author_title: Engineer @ BuildBuddy
date: 2023-05-26:12:00:00
author_url: https://www.linkedin.com/in/vadimberezniker/
author_image_url: https://avatars.githubusercontent.com/u/1335358?v=4
tags: [product security encryption enterprise]
---

BuildBuddy enables fast builds by providing a high-performance cache that stores and serves artifacts, such as source
code.

Starting today, for extra peace of mind customers can provide their own encryption key that will be used to encrypt
and decrypt data stored in the cache. At launch, we are supporting keys managed by Google Cloud Platform KMS and Amazon
Web Services KMS.

<!-- truncate -->

To get started, head over to your organization Settings page and look for the "Encryption Keys" tab. Note that these
settings are only visible to Organization Administrators.

For more details, see the [Customer Managed Encryption Keys documentation](https://www.buildbuddy.io/docs/remote-build-execution).

If you have any questions or feedback about this feature, please reach out to us on [Slack](https://slack.buildbuddy.io/) or at [hello@buildbuddy.io](mailto:hello@buildbuddy.io).

## Technical Details

The supplied KMS key is not used directly to encrypt and decrypt data as that would require all cache operations to
stream data to and from the KMS which would affect performance as well as incur additional KMS costs. Instead,
when the feature is enabled, BuildBuddy internally generates a pair of 256-bit symmetric keys which are themselves encrypted
using a combination of the customer-supplied key and a BuildBuddy-managed key. Revocation of either key renders
previously written artifacts undecryptable.

The concatenation of the two internal keys is fed into HKDF-Expand using a SHA256 hash to derive a single 256-bit key
that is used for cryptographic operations on the cache contents.

When cache reads and writes are performed, we use the XChaCha20-Poly1305 algorithm to encrypt, decrypt and authenticate
the data using the derived key described above.
