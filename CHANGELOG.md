# Changelog

Release notes for `flink-proto-confluent`. This file is maintained by [release-please](https://github.com/googleapis/release-please) from the Conventional Commit messages on `main` — edit the commits, not this file. See [docs/RELEASING.md](docs/RELEASING.md) for how a release is cut.

## [1.1.0](https://github.com/brbrown25/flink-proto-confluent/compare/v1.0.0...v1.1.0) (2026-09-23)


### Features

* **release:** drive releases from a release-please pull request. [#114](https://github.com/brbrown25/flink-proto-confluent/issues/114). ([5d429e5](https://github.com/brbrown25/flink-proto-confluent/commit/5d429e5c7a6d411ef5a9af7adf7a31224efe5973))
* **schema-evolution:** add evolution, compatibility and subject-naming coverage ([8eda07e](https://github.com/brbrown25/flink-proto-confluent/commit/8eda07e39515f6532823ca55df57d5f2770853ea)), closes [#70](https://github.com/brbrown25/flink-proto-confluent/issues/70)
* **tests:** add auth precedence and negative-validation config tests. [#71](https://github.com/brbrown25/flink-proto-confluent/issues/71). ([9f1e212](https://github.com/brbrown25/flink-proto-confluent/commit/9f1e212a3e29fffb15576f69812e3a166b69cc4d))
* **tests:** add TLS/mTLS Schema Registry integration tests. [#68](https://github.com/brbrown25/flink-proto-confluent/issues/68). ([d86dd0d](https://github.com/brbrown25/flink-proto-confluent/commit/d86dd0df424baa00e9a3055d7b4b1210b0431db7))
* **tests:** adding in authenticated Schema Registry integration tests. [#67](https://github.com/brbrown25/flink-proto-confluent/issues/67). ([c794102](https://github.com/brbrown25/flink-proto-confluent/commit/c794102cbd396eb2132f0c944ebfd2796ebf2d12))
* **tests:** adding in dead-letter topic produce and deserialize-error metrics tests. [#68](https://github.com/brbrown25/flink-proto-confluent/issues/68). ([222b026](https://github.com/brbrown25/flink-proto-confluent/commit/222b0261e4d2eac8d0b3c60ecfd5fe860aed8956))


### Dependencies

* **gradle:** bump com.clickhouse:clickhouse-jdbc from 0.4.6 to 0.10.0 ([0fd08f7](https://github.com/brbrown25/flink-proto-confluent/commit/0fd08f78152dbf20c5d60d03ee29c220a5611b4b))
* **gradle:** bump com.diffplug.spotless from 8.10.1 to 8.10.2 ([b0ac7f3](https://github.com/brbrown25/flink-proto-confluent/commit/b0ac7f3a3c3faed38383cff4f0151cbe80a15303))
* **gradle:** bump com.diffplug.spotless from 8.8.0 to 8.9.0 ([f3d48b7](https://github.com/brbrown25/flink-proto-confluent/commit/f3d48b7194b138b4c936e9af659ddc53015bf82f))
* **gradle:** bump com.diffplug.spotless from 8.9.0 to 8.10.1 ([bc5b7ba](https://github.com/brbrown25/flink-proto-confluent/commit/bc5b7ba693b72bf9d4881edc28f5dc9225922080))
* **gradle:** bump com.github.spotbugs from 6.5.10 to 6.5.11 ([a77f016](https://github.com/brbrown25/flink-proto-confluent/commit/a77f0160bfbd25d1d4376d30b2507f46c955304d))
* **gradle:** bump com.github.spotbugs from 6.5.8 to 6.5.9 ([4afecba](https://github.com/brbrown25/flink-proto-confluent/commit/4afecbadb6ebc087d7cf1d2d6cc0604a518bbb96))
* **gradle:** bump com.github.spotbugs from 6.5.9 to 6.5.10 ([606aef4](https://github.com/brbrown25/flink-proto-confluent/commit/606aef4d2c5b0d4e64f05b2451c35a562b975b61))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([250db45](https://github.com/brbrown25/flink-proto-confluent/commit/250db45367e7dd32a0bf5f6d00714ccf36376182))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([f1b086e](https://github.com/brbrown25/flink-proto-confluent/commit/f1b086eab6156d044922220b99d7ee6d7d2c9b92))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([5ecddec](https://github.com/brbrown25/flink-proto-confluent/commit/5ecddecb06ecfc879963112a05df6341fc45fd9e))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([f1e87fa](https://github.com/brbrown25/flink-proto-confluent/commit/f1e87fa5525a7a80b77bc57d7cdf4cc74e11bd5f))
* **gradle:** bump com.gradleup.shadow from 9.5.1 to 9.6.1 ([e74bbcd](https://github.com/brbrown25/flink-proto-confluent/commit/e74bbcdb7763bd8420a99bb09baeee117e5d90b2))
* **gradle:** bump gradle-wrapper from 9.6.1 to 9.7.0 ([ca7a0ae](https://github.com/brbrown25/flink-proto-confluent/commit/ca7a0aec05103eea3cacadaa4f36ca2b77bc3057))
* **gradle:** bump gradle-wrapper from 9.7.0 to 9.7.1 ([b0da0ae](https://github.com/brbrown25/flink-proto-confluent/commit/b0da0ae5b844fb3e747e21f384a24f1aa008abd2))
* **gradle:** bump org.apache.flink:flink-connector-jdbc ([d673308](https://github.com/brbrown25/flink-proto-confluent/commit/d673308d65f3c4882a74cfaf852d42d02350f389))
* **gradle:** bump the testing group with 2 updates ([09f6b8e](https://github.com/brbrown25/flink-proto-confluent/commit/09f6b8e2ca2d7c39b6fddd5b1aff2fc209a483fa))
* **gradle:** bump the testing group with 2 updates ([a46d7e4](https://github.com/brbrown25/flink-proto-confluent/commit/a46d7e44f704bb5f91c0c0f53a6a284c2d76e150))

## 1.0.0 (2026-07-13)

Initial release. See the [v1.0.0 release notes](https://github.com/brbrown25/flink-proto-confluent/releases/tag/v1.0.0).
