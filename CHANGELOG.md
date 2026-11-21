# Changelog

Release notes for `flink-proto-confluent`. This file is maintained by [release-please](https://github.com/googleapis/release-please) from the Conventional Commit messages on `main` — edit the commits, not this file. See [docs/RELEASING.md](docs/RELEASING.md) for how a release is cut.

## [1.3.0](https://github.com/brbrown25/flink-proto-confluent/compare/v1.2.0...v1.3.0) (2026-11-21)


### Features

* **bench:** add JMH benchmarks for the per-record deserialize and serialize paths. [#180](https://github.com/brbrown25/flink-proto-confluent/issues/180) ([2a33415](https://github.com/brbrown25/flink-proto-confluent/commit/2a33415106a1eeb6c934db967ee7f51dcd21b9ef))
* **build:** check the public API against the last release with japicmp on every pull request. [#220](https://github.com/brbrown25/flink-proto-confluent/issues/220) ([b52750b](https://github.com/brbrown25/flink-proto-confluent/commit/b52750bf34e0e7e54630142059cc80a074ab4512))
* **build:** select the Flink line with -PflinkVersion. [#147](https://github.com/brbrown25/flink-proto-confluent/issues/147) ([084e02a](https://github.com/brbrown25/flink-proto-confluent/commit/084e02a68c1bcbc302af54f85c2117a1b07a1edb)), closes [#142](https://github.com/brbrown25/flink-proto-confluent/issues/142)
* **catalog:** cover the per-subject listing's error path, which the default constructor no longer reaches. [#247](https://github.com/brbrown25/flink-proto-confluent/issues/247) ([7833c88](https://github.com/brbrown25/flink-proto-confluent/commit/7833c88ef4548ae14a1579566ad337c321c2d349))
* **catalog:** expose Schema Registry protobuf topics as tables through a catalog. [#169](https://github.com/brbrown25/flink-proto-confluent/issues/169) ([fd7005a](https://github.com/brbrown25/flink-proto-confluent/commit/fd7005a43c1e262ec6bedd6a5916fa4e322f5b6c))
* **catalog:** list tables with one bulk registry request per page instead of one per subject, and add topics.include and topics.exclude filters. [#247](https://github.com/brbrown25/flink-proto-confluent/issues/247) ([2301cf8](https://github.com/brbrown25/flink-proto-confluent/commit/2301cf8512613480d73e4f36efb3691426ed69fc))
* **ci:** build and test every PR on Flink 1.20 and 2.x. [#147](https://github.com/brbrown25/flink-proto-confluent/issues/147) ([07574cc](https://github.com/brbrown25/flink-proto-confluent/commit/07574cce692eefe32fbd95ad9f9020ef810fd997)), closes [#144](https://github.com/brbrown25/flink-proto-confluent/issues/144)
* **ci:** prove each CI leg ran on the Flink line it names, and lint both lines. [#147](https://github.com/brbrown25/flink-proto-confluent/issues/147) ([75d9ff2](https://github.com/brbrown25/flink-proto-confluent/commit/75d9ff2559a37ed64bab4fb4439c2ef783a6df72)), closes [#144](https://github.com/brbrown25/flink-proto-confluent/issues/144)
* **cli:** generate CREATE TABLE DDL from a Schema Registry subject. [#168](https://github.com/brbrown25/flink-proto-confluent/issues/168) ([70e71ef](https://github.com/brbrown25/flink-proto-confluent/commit/70e71ef006a4b40a6d92462b02e9ca8aa43c5d7a))
* **compat:** compile and run the shared source on Flink 1.20 and 2.x. [#147](https://github.com/brbrown25/flink-proto-confluent/issues/147) ([6a4df3b](https://github.com/brbrown25/flink-proto-confluent/commit/6a4df3b4f564b3fc964c1b816b0ed9347dc3f4fc)), closes [#143](https://github.com/brbrown25/flink-proto-confluent/issues/143)
* **config:** pin ProtoConfluentFormatConfig's serialVersionUID to its 1.2.0 value. [#220](https://github.com/brbrown25/flink-proto-confluent/issues/220) ([6f1ae5f](https://github.com/brbrown25/flink-proto-confluent/commit/6f1ae5f108be62d0abae1ecca2042ea7ba68ee5e))
* **deserialize:** honour message-class and message-name on the source. [#177](https://github.com/brbrown25/flink-proto-confluent/issues/177) ([1b770f4](https://github.com/brbrown25/flink-proto-confluent/commit/1b770f4076b23edc7f9ea27baa1d6f9882badb71))
* **deserialize:** keep the failure-log test schema's fields non-transient so SpotBugs accepts it. [#233](https://github.com/brbrown25/flink-proto-confluent/issues/233) ([a4aff92](https://github.com/brbrown25/flink-proto-confluent/commit/a4aff9286cf910ae1dcab24b43ac4d687a1ad46d))
* **deserialize:** log the first deserialize failure with its stack trace and then at most one a minute, with the count suppressed in between. [#233](https://github.com/brbrown25/flink-proto-confluent/issues/233) ([7aa4108](https://github.com/brbrown25/flink-proto-confluent/commit/7aa41081f0406c57d6c88781f38dacf615cf947e))
* **deserialize:** read int32 fields into BIGINT, float into DOUBLE and integers into a wide enough DECIMAL, so a table declared for a widened field reads records of the older schema. [#245](https://github.com/brbrown25/flink-proto-confluent/issues/245) ([5fb7c86](https://github.com/brbrown25/flink-proto-confluent/commit/5fb7c86eaa176f9e8601626ec98a3fa1f76fe6f1))
* **docs:** document the Flink 1.20 and 2.x support matrix. [#147](https://github.com/brbrown25/flink-proto-confluent/issues/147) ([c9ee87b](https://github.com/brbrown25/flink-proto-confluent/commit/c9ee87bca6d32d39633fb9d074ffaf2596fa80de)), closes [#146](https://github.com/brbrown25/flink-proto-confluent/issues/146)
* **format:** add subject and subject-name-strategy options and make topic optional. [#176](https://github.com/brbrown25/flink-proto-confluent/issues/176) ([cae0241](https://github.com/brbrown25/flink-proto-confluent/commit/cae02415baa0b38b8caa7749cc504c6d1feaefe2))
* **format:** reject a key format without is_key and key and value formats that resolve the same subject. [#241](https://github.com/brbrown25/flink-proto-confluent/issues/241) ([8c0cef0](https://github.com/brbrown25/flink-proto-confluent/commit/8c0cef0eee27bf9edb8db551505c28124fd9631e))
* **registry:** close the Schema Registry client of each source and sink when the job leaves the TaskManager. [#248](https://github.com/brbrown25/flink-proto-confluent/issues/248) ([10ebb3c](https://github.com/brbrown25/flink-proto-confluent/commit/10ebb3cd9e7ba3468df90bbeefb9e475963ffd8d))
* **registry:** create the recorded clients lazily in the close tests, so that SpotBugs does not flag a transient field set by the constructor. [#248](https://github.com/brbrown25/flink-proto-confluent/issues/248) ([0e2a593](https://github.com/brbrown25/flink-proto-confluent/commit/0e2a593141ec1453869335062069eb3bf364f2a4))
* **release:** publish one artifact per Flink line. [#147](https://github.com/brbrown25/flink-proto-confluent/issues/147) ([9e931e0](https://github.com/brbrown25/flink-proto-confluent/commit/9e931e056892d1b9669d6ad2d7d99bae981737ac)), closes [#145](https://github.com/brbrown25/flink-proto-confluent/issues/145)
* **security:** read ddl credentials from an environment variable or a file, strip user info from logged registry URLs and document that catalog tables carry the credentials. [#240](https://github.com/brbrown25/flink-proto-confluent/issues/240) ([4489035](https://github.com/brbrown25/flink-proto-confluent/commit/4489035bb4233666e7985f915a115fdfc90fde30))
* **serialize:** add derived-message-name to name the message a sink derives from the table. [#246](https://github.com/brbrown25/flink-proto-confluent/issues/246) ([3655e02](https://github.com/brbrown25/flink-proto-confluent/commit/3655e0238895242fd77aeb14166981f90ad110ce))
* **serialize:** load the sink's message-class with the job's user-code class loader, as the source does. [#242](https://github.com/brbrown25/flink-proto-confluent/issues/242) ([fa81f0f](https://github.com/brbrown25/flink-proto-confluent/commit/fa81f0f9c52fae63180276ed1cd7b98e5550d2eb))
* **serialize:** match snake_case columns to camelCase fields on the sink, as the source does. [#243](https://github.com/brbrown25/flink-proto-confluent/issues/243) ([6ba9c6a](https://github.com/brbrown25/flink-proto-confluent/commit/6ba9c6a7068976795b24683a2e7344547b4dd01e))
* **serialize:** satisfy SpotBugs in the class loader test by not dereferencing getParent() and building the job loader in a helper. [#242](https://github.com/brbrown25/flink-proto-confluent/issues/242) ([705c2f3](https://github.com/brbrown25/flink-proto-confluent/commit/705c2f3c1033e93829907fc2899c8d7f8ebca26d))
* **serialize:** write the message-name message of the subject's schema when it defines several messages. [#244](https://github.com/brbrown25/flink-proto-confluent/issues/244) ([9045760](https://github.com/brbrown25/flink-proto-confluent/commit/9045760f8cb6254132952d5141d18bc55aae5498))
* **source:** push nested projections into the proto-confluent decoder. [#217](https://github.com/brbrown25/flink-proto-confluent/issues/217) ([cca9bf3](https://github.com/brbrown25/flink-proto-confluent/commit/cca9bf3d98977f581e1572c1b3a2b310d0226776))
* **source:** push query projections into the proto-confluent decoder. [#173](https://github.com/brbrown25/flink-proto-confluent/issues/173) ([91d2e61](https://github.com/brbrown25/flink-proto-confluent/commit/91d2e6156497922e581e5a8b19dbbaa10606a2a8))
* **types:** derive DDL for recursive messages with an opt-in recursive-message option. [#216](https://github.com/brbrown25/flink-proto-confluent/issues/216) ([efe246f](https://github.com/brbrown25/flink-proto-confluent/commit/efe246f031980ca6f6e53cfa97ac67de2b82142b))
* **types:** map google.protobuf.Struct, Value and ListValue to a JSON STRING. [#174](https://github.com/brbrown25/flink-proto-confluent/issues/174) ([3efd90c](https://github.com/brbrown25/flink-proto-confluent/commit/3efd90c1eb144a4b3c7725ece563207863237173))
* **types:** read and write an enum as its number when the column is INT. [#174](https://github.com/brbrown25/flink-proto-confluent/issues/174) ([d20cf62](https://github.com/brbrown25/flink-proto-confluent/commit/d20cf620e72a309eb3c1a176dc39446f7653b7ba))
* **types:** read and write google.protobuf.Duration as INTERVAL DAY TO SECOND. [#174](https://github.com/brbrown25/flink-proto-confluent/issues/174) ([08245d0](https://github.com/brbrown25/flink-proto-confluent/commit/08245d0e547c298c761c317497f330a6fb69a47c))
* **types:** read and write google.protobuf.Timestamp as TIMESTAMP without time zone. [#174](https://github.com/brbrown25/flink-proto-confluent/issues/174) ([ef885b8](https://github.com/brbrown25/flink-proto-confluent/commit/ef885b8f2376a9ba8181cfb274ff93903d18942a))
* **types:** read and write recursive messages as BYTES, JSON or a fixed-depth ROW. [#174](https://github.com/brbrown25/flink-proto-confluent/issues/174) ([2e84cf0](https://github.com/brbrown25/flink-proto-confluent/commit/2e84cf0b7f27cd8948c44ac7d96c752d24ed4047))
* **types:** read and write uint64 and fixed64 as DECIMAL(20, 0) without wrapping. [#174](https://github.com/brbrown25/flink-proto-confluent/issues/174) ([276caa8](https://github.com/brbrown25/flink-proto-confluent/commit/276caa8e1edacdf0f9970e5c7e6cb560167faea1))


### Bug Fixes

* **ddl:** declare protobuf bytes fields as BYTES in generated DDL. [#222](https://github.com/brbrown25/flink-proto-confluent/issues/222) ([05374af](https://github.com/brbrown25/flink-proto-confluent/commit/05374af27c95351297b93ffa0f61063f49f570dd))
* **deserialize:** fail the record when a TINYINT or SMALLINT column gets an out-of-range int32 value. [#235](https://github.com/brbrown25/flink-proto-confluent/issues/235) ([b899467](https://github.com/brbrown25/flink-proto-confluent/commit/b8994671e7201d0aac76cf928c1cf88625091cb3))
* **deserialize:** fail the task on a Schema Registry outage instead of skipping or dead-lettering the record. [#232](https://github.com/brbrown25/flink-proto-confluent/issues/232) ([50b3e5d](https://github.com/brbrown25/flink-proto-confluent/commit/50b3e5d088e3efd0486b0397710d5e50e64a374b))
* **deserialize:** match oneof columns to their cases by name on read, as the sink does. [#231](https://github.com/brbrown25/flink-proto-confluent/issues/231) ([56562b0](https://github.com/brbrown25/flink-proto-confluent/commit/56562b0b44bb4c5378734ce2b8d03ae1e71258bf))
* **deserialize:** put the @TempDir annotation in MainTest on its own line, as Checkstyle asks. [#231](https://github.com/brbrown25/flink-proto-confluent/issues/231) ([127a504](https://github.com/brbrown25/flink-proto-confluent/commit/127a504bb2799d6fceb5fb568356f4c00c9f430a))
* **deserialize:** read an empty google.type.Date as NULL and fail a partial one with an error naming the field. [#236](https://github.com/brbrown25/flink-proto-confluent/issues/236) ([9848692](https://github.com/brbrown25/flink-proto-confluent/commit/9848692c36c3794d9893604b2f75b5e54959f2e6))
* **deserialize:** read the google.type.Date parts by name instead of a switch with an unreachable branch, and test a date with neither year nor month. [#236](https://github.com/brbrown25/flink-proto-confluent/issues/236) ([aaff2c4](https://github.com/brbrown25/flink-proto-confluent/commit/aaff2c4d006f9a5eba3f68422b73b6427490432d))
* **deserialize:** unit-test the registry outage classification for every status and cause chain, and rewrap the on-deserialize-error Javadoc Checkstyle flagged. [#232](https://github.com/brbrown25/flink-proto-confluent/issues/232) ([554f52c](https://github.com/brbrown25/flink-proto-confluent/commit/554f52c15b7b3f58f2d4312b9d59e69194631a83))
* **serialize:** derive NOT NULL columns as plain proto3 fields, not required ones. [#237](https://github.com/brbrown25/flink-proto-confluent/issues/237) ([ec23d90](https://github.com/brbrown25/flink-proto-confluent/commit/ec23d90fdeabd989337520b71a2801f14e74b18c))
* **serialize:** drop the NULL guards no caller can reach from the 32-bit narrowing converters, and put @TempDir on its own line for Checkstyle. [#234](https://github.com/brbrown25/flink-proto-confluent/issues/234) ([1178a58](https://github.com/brbrown25/flink-proto-confluent/commit/1178a580cb7bb3cc77138c7d33ece79713171c61))
* **serialize:** drop the unused IntType import that Checkstyle flagged in the invalid-value test. [#234](https://github.com/brbrown25/flink-proto-confluent/issues/234) ([af0cc4c](https://github.com/brbrown25/flink-proto-confluent/commit/af0cc4c9c69cdb17964eeb88bfb1baf5016d641c))
* **serialize:** fail with a clear error on unknown enum names, NULL collection entries and out-of-range BIGINTs for 32-bit fields. [#234](https://github.com/brbrown25/flink-proto-confluent/issues/234) ([607a172](https://github.com/brbrown25/flink-proto-confluent/commit/607a172f359eb7e7fceb58211bf9af961e2fa704))
* **serialize:** strip user info from the Schema Registry URL in the sink's close-failure log. [#240](https://github.com/brbrown25/flink-proto-confluent/issues/240) ([8887e7c](https://github.com/brbrown25/flink-proto-confluent/commit/8887e7c914e14a5ee50c626c76e97916c679c4e4))
* **serialize:** write pre-1970 timestamps with non-negative nanos. [#230](https://github.com/brbrown25/flink-proto-confluent/issues/230) ([88068bc](https://github.com/brbrown25/flink-proto-confluent/commit/88068bc3ecf06de3b64ae91c0883519c2b11dfb7))
* **sink:** write under the schema ID read from the registry instead of looking up a rebuilt schema. [#210](https://github.com/brbrown25/flink-proto-confluent/issues/210) ([74bebd3](https://github.com/brbrown25/flink-proto-confluent/commit/74bebd3d223a838056f70091b9cdac460462a970))
* **source:** convert each record with the converter of its own message, not of the first message seen under its schema ID. [#211](https://github.com/brbrown25/flink-proto-confluent/issues/211) ([2a5c24c](https://github.com/brbrown25/flink-proto-confluent/commit/2a5c24c42cba047ebdbfb3e0be1b93c37fb46436))
* **source:** leave the source.topic dead-letter header out when the source has no topic instead of writing "null". [#212](https://github.com/brbrown25/flink-proto-confluent/issues/212) ([32ddbe5](https://github.com/brbrown25/flink-proto-confluent/commit/32ddbe5276453c8d282078cc21457683da200a26))
* **types:** refuse TIMESTAMP or INTERVAL for a field that is not a Timestamp or Duration when the reader is built. [#174](https://github.com/brbrown25/flink-proto-confluent/issues/174) ([8122698](https://github.com/brbrown25/flink-proto-confluent/commit/8122698811206d998fafd421cc2e0067de229912))


### Dependencies

* **actions:** bump dorny/test-reporter from 3.0.0 to 3.2.0 ([517f39e](https://github.com/brbrown25/flink-proto-confluent/commit/517f39e5ffaf0ea4ab2ce1af5dcb37e42b416dba))
* **actions:** bump gradle/actions/setup-gradle ([bd60878](https://github.com/brbrown25/flink-proto-confluent/commit/bd60878a40f519060485295c2ce6be6d3e975457))
* **gradle:** bump com.clickhouse:clickhouse-jdbc from 0.11.0 to 0.11.1 ([7cc2e34](https://github.com/brbrown25/flink-proto-confluent/commit/7cc2e34494041577e7f759f1455bb373c0bf5d3c))
* **gradle:** bump com.clickhouse:clickhouse-jdbc from 0.11.1 to 0.11.2 ([cbf9711](https://github.com/brbrown25/flink-proto-confluent/commit/cbf97117f3d554784265914f9eb40416f03c21c7))
* **gradle:** bump gradle-wrapper from 9.9.0 to 9.9.1 ([c44d345](https://github.com/brbrown25/flink-proto-confluent/commit/c44d3458d96be6a75470208b9c79c66ada83d4fe))
* **gradle:** bump me.champeau.jmh from 0.7.3 to 0.7.4 ([36144ff](https://github.com/brbrown25/flink-proto-confluent/commit/36144ff87f55162b034a07c962e646442c61fd15))
* **gradle:** bump org.apache.commons:commons-lang3 ([ab8f4c4](https://github.com/brbrown25/flink-proto-confluent/commit/ab8f4c411fe334f6ff0f8a428bb9f64e63027082))

## [1.2.0](https://github.com/brbrown25/flink-proto-confluent/compare/v1.1.0...v1.2.0) (2026-11-16)


### Features

* **build:** publish a thin library JAR and a relocated -all JAR. NOTICKET. ([8656dbc](https://github.com/brbrown25/flink-proto-confluent/commit/8656dbc13255e1c6dc6e167fa440ada15d363b70))
* **ci:** mergify auto-merges passing dependabot patch/minor PRs. NOTICKET. ([36a9daf](https://github.com/brbrown25/flink-proto-confluent/commit/36a9daffcbff8df1ef6e8376efa24060ebe5c40a))
* **docs:** add MkDocs Material docs site with asciinema demos. NOTICKET. ([ad04b39](https://github.com/brbrown25/flink-proto-confluent/commit/ad04b39b850407710d44f0b1ddd536869b877b15))
* **docs:** clarify transitive compat check limits, add check --all-versions, guard test. NOTICKET. ([f5ee673](https://github.com/brbrown25/flink-proto-confluent/commit/f5ee673fd01f52a84981dd3e3150cd23e61a1b97))
* **docs:** compat-modes cast demonstrates check --all-versions vs registration. NOTICKET. ([a3aee85](https://github.com/brbrown25/flink-proto-confluent/commit/a3aee8555c9a43d434eb0cc4e18c9d665f705006))
* **docs:** record.sh accepts cast names and reuses a healthy stack. NOTICKET. ([37787b8](https://github.com/brbrown25/flink-proto-confluent/commit/37787b89d60424d30e3928c29228720d9f20060e)), closes [#129](https://github.com/brbrown25/flink-proto-confluent/issues/129)
* **docs:** release-please version markers, PR checklist, contributor docs guide. NOTICKET. ([2a60fe9](https://github.com/brbrown25/flink-proto-confluent/commit/2a60fe9f8c0e4f9fe45e89f22eff87131880f6da))
* **docs:** schema evolution lifecycle docs, compat-mode demos, and integration test. NOTICKET. ([b981972](https://github.com/brbrown25/flink-proto-confluent/commit/b9819724ffec1266dc5fd77097d8dc94dff7fdea)), closes [#133](https://github.com/brbrown25/flink-proto-confluent/issues/133)
* **metrics:** add serialize-error, Schema Registry and cached-schema metrics. [#175](https://github.com/brbrown25/flink-proto-confluent/issues/175) ([4bc4e90](https://github.com/brbrown25/flink-proto-confluent/commit/4bc4e905a9a696258c5a1c47581815de156244d7))
* **source:** make dead-letter delivery observable, add dead-letter.delivery sync mode and close the producer. [#171](https://github.com/brbrown25/flink-proto-confluent/issues/171) ([2b1a215](https://github.com/brbrown25/flink-proto-confluent/commit/2b1a215e51a1f862d4d9f406fa2d954222d513b8))


### Bug Fixes

* **build:** declare the protobuf-java version the library needs and guard it. [#189](https://github.com/brbrown25/flink-proto-confluent/issues/189) ([143880f](https://github.com/brbrown25/flink-proto-confluent/commit/143880febcdcceed1dda4974055faec72d117726))
* **ci:** mergify merges dependabot PRs with rebase since the ruleset allows only rebase. NOTICKET. ([553dff0](https://github.com/brbrown25/flink-proto-confluent/commit/553dff0403b803ab01e388e750715b0a7cb3e653))
* **config:** validate on-deserialize-error as an enum so a typo no longer means skip. [#170](https://github.com/brbrown25/flink-proto-confluent/issues/170) ([0e3d064](https://github.com/brbrown25/flink-proto-confluent/commit/0e3d0642756153f4645fb5cb35ca6508d7ad5be9))
* **serialize:** add integration test for sink column missing from registered schema. NOTICKET. ([13dcc3a](https://github.com/brbrown25/flink-proto-confluent/commit/13dcc3ab74f1c0209f2d9513c1187bddba3d6832))
* **serialize:** fail fast with descriptive error when sink column is missing from target schema. NOTICKET. ([d855303](https://github.com/brbrown25/flink-proto-confluent/commit/d8553036f53cd06e587839f96216d1c204be67bb))
* **serialize:** simplify oneof column validation and cover remaining branches. NOTICKET. ([0e87926](https://github.com/brbrown25/flink-proto-confluent/commit/0e87926a2752c4cca9b469b95c3d53fa4bc12b23))
* **sink:** fail on registry errors instead of falling back to a Row-derived schema, add schema-cache.ttl. [#172](https://github.com/brbrown25/flink-proto-confluent/issues/172) ([d1b2f50](https://github.com/brbrown25/flink-proto-confluent/commit/d1b2f5088a84493640819a988a0396641d600454))


### Dependencies

* **actions:** bump actions/checkout from 4 to 7 ([5757784](https://github.com/brbrown25/flink-proto-confluent/commit/575778455f396b18491dcd1b9c5f4bbaf8c09f90))
* **actions:** bump actions/deploy-pages from 4 to 5 ([8a1626b](https://github.com/brbrown25/flink-proto-confluent/commit/8a1626be465864fde34b6997ec135f0fec774064))
* **actions:** bump actions/setup-python from 5 to 7 ([23c52ea](https://github.com/brbrown25/flink-proto-confluent/commit/23c52ea10fa3dcd189c5ddfe92a9a21cff616b0d))
* **actions:** bump actions/upload-pages-artifact from 3 to 5 ([203f6d2](https://github.com/brbrown25/flink-proto-confluent/commit/203f6d28c7db013962f46c70a8415bd65ad369d5))
* **gradle:** bump com.clickhouse:clickhouse-jdbc from 0.10.0 to 0.11.0 ([df898fe](https://github.com/brbrown25/flink-proto-confluent/commit/df898feead77ebeb96c8446c61cb09697285a711))
* **gradle:** bump com.diffplug.spotless from 8.10.3 to 8.10.4 ([7d64e0a](https://github.com/brbrown25/flink-proto-confluent/commit/7d64e0a9818f218422f55abf5445f8e71af5e196))
* **gradle:** bump com.diffplug.spotless from 8.10.4 to 8.11.0 ([57036f5](https://github.com/brbrown25/flink-proto-confluent/commit/57036f5c569a65a54bf2ad7a4a4e3aa2df897dbc))
* **gradle:** bump com.github.spotbugs from 6.5.11 to 6.5.12 ([aaa754a](https://github.com/brbrown25/flink-proto-confluent/commit/aaa754aa846ebe9325fd800e7950e3bf71639627))
* **gradle:** bump com.github.spotbugs from 6.5.12 to 6.5.13 ([8ecda39](https://github.com/brbrown25/flink-proto-confluent/commit/8ecda399719d87ee84f7df8a2eefbff7e111ed3a))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([342e67a](https://github.com/brbrown25/flink-proto-confluent/commit/342e67ae386481a4bc9cd4572e82e7488293cbfb))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([247d3ce](https://github.com/brbrown25/flink-proto-confluent/commit/247d3ce0bf5ab830c272cd2c4435e12d6013dcab))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([22ed421](https://github.com/brbrown25/flink-proto-confluent/commit/22ed421a4453d7ce26d88b4cf6fd179756834ce7))
* **gradle:** bump gradle-wrapper from 9.8.0 to 9.8.1 ([e4945cc](https://github.com/brbrown25/flink-proto-confluent/commit/e4945cce1b6372cdc056b11ed3b7317ebd94c46f))
* **gradle:** bump gradle-wrapper from 9.8.1 to 9.8.2 ([c2da634](https://github.com/brbrown25/flink-proto-confluent/commit/c2da6341d1c354be49bae373b630df42b0894c5d))
* **gradle:** bump gradle-wrapper from 9.8.2 to 9.9.0 ([4d3ff61](https://github.com/brbrown25/flink-proto-confluent/commit/4d3ff614c13dc7e4886ac0d73c5b6d0a98b1d983))
* **gradle:** bump org.apache.commons:commons-lang3 ([7219d40](https://github.com/brbrown25/flink-proto-confluent/commit/7219d400d34f7d73243c134cc4b03c865f626cdb))

## [1.1.0](https://github.com/brbrown25/flink-proto-confluent/compare/v1.0.0...v1.1.0) (2026-10-03)


### Features

* **release:** drive releases from a release-please pull request. [#114](https://github.com/brbrown25/flink-proto-confluent/issues/114). ([5d429e5](https://github.com/brbrown25/flink-proto-confluent/commit/5d429e5c7a6d411ef5a9af7adf7a31224efe5973))
* **schema-evolution:** add evolution, compatibility and subject-naming coverage ([8eda07e](https://github.com/brbrown25/flink-proto-confluent/commit/8eda07e39515f6532823ca55df57d5f2770853ea)), closes [#70](https://github.com/brbrown25/flink-proto-confluent/issues/70)
* **tests:** add auth precedence and negative-validation config tests. [#71](https://github.com/brbrown25/flink-proto-confluent/issues/71). ([9f1e212](https://github.com/brbrown25/flink-proto-confluent/commit/9f1e212a3e29fffb15576f69812e3a166b69cc4d))
* **tests:** add TLS/mTLS Schema Registry integration tests. [#68](https://github.com/brbrown25/flink-proto-confluent/issues/68). ([d86dd0d](https://github.com/brbrown25/flink-proto-confluent/commit/d86dd0df424baa00e9a3055d7b4b1210b0431db7))
* **tests:** adding in authenticated Schema Registry integration tests. [#67](https://github.com/brbrown25/flink-proto-confluent/issues/67). ([c794102](https://github.com/brbrown25/flink-proto-confluent/commit/c794102cbd396eb2132f0c944ebfd2796ebf2d12))
* **tests:** adding in dead-letter topic produce and deserialize-error metrics tests. [#68](https://github.com/brbrown25/flink-proto-confluent/issues/68). ([222b026](https://github.com/brbrown25/flink-proto-confluent/commit/222b0261e4d2eac8d0b3c60ecfd5fe860aed8956))


### Dependencies

* **actions:** bump actions/download-artifact from 7 to 8 ([e9ee00e](https://github.com/brbrown25/flink-proto-confluent/commit/e9ee00e230085fa9ae1d1e2c54cb2b3335428fb8))
* **actions:** bump googleapis/release-please-action from 4 to 5 ([30eede2](https://github.com/brbrown25/flink-proto-confluent/commit/30eede24a8dc1c88dc27cbbde74f5a433998d084))
* **gradle:** bump com.clickhouse:clickhouse-jdbc from 0.4.6 to 0.10.0 ([0fd08f7](https://github.com/brbrown25/flink-proto-confluent/commit/0fd08f78152dbf20c5d60d03ee29c220a5611b4b))
* **gradle:** bump com.diffplug.spotless from 8.10.1 to 8.10.2 ([b0ac7f3](https://github.com/brbrown25/flink-proto-confluent/commit/b0ac7f3a3c3faed38383cff4f0151cbe80a15303))
* **gradle:** bump com.diffplug.spotless from 8.10.2 to 8.10.3 ([3d72523](https://github.com/brbrown25/flink-proto-confluent/commit/3d72523929c7c4b696ad4be0687d3cc734aa81f5))
* **gradle:** bump com.diffplug.spotless from 8.8.0 to 8.9.0 ([f3d48b7](https://github.com/brbrown25/flink-proto-confluent/commit/f3d48b7194b138b4c936e9af659ddc53015bf82f))
* **gradle:** bump com.diffplug.spotless from 8.9.0 to 8.10.1 ([bc5b7ba](https://github.com/brbrown25/flink-proto-confluent/commit/bc5b7ba693b72bf9d4881edc28f5dc9225922080))
* **gradle:** bump com.github.spotbugs from 6.5.10 to 6.5.11 ([a77f016](https://github.com/brbrown25/flink-proto-confluent/commit/a77f0160bfbd25d1d4376d30b2507f46c955304d))
* **gradle:** bump com.github.spotbugs from 6.5.8 to 6.5.9 ([4afecba](https://github.com/brbrown25/flink-proto-confluent/commit/4afecbadb6ebc087d7cf1d2d6cc0604a518bbb96))
* **gradle:** bump com.github.spotbugs from 6.5.9 to 6.5.10 ([606aef4](https://github.com/brbrown25/flink-proto-confluent/commit/606aef4d2c5b0d4e64f05b2451c35a562b975b61))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([f8afc7a](https://github.com/brbrown25/flink-proto-confluent/commit/f8afc7a7864003998a01360035593256b836928d))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([250db45](https://github.com/brbrown25/flink-proto-confluent/commit/250db45367e7dd32a0bf5f6d00714ccf36376182))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([f1b086e](https://github.com/brbrown25/flink-proto-confluent/commit/f1b086eab6156d044922220b99d7ee6d7d2c9b92))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([5ecddec](https://github.com/brbrown25/flink-proto-confluent/commit/5ecddecb06ecfc879963112a05df6341fc45fd9e))
* **gradle:** bump com.google.api.grpc:proto-google-common-protos ([f1e87fa](https://github.com/brbrown25/flink-proto-confluent/commit/f1e87fa5525a7a80b77bc57d7cdf4cc74e11bd5f))
* **gradle:** bump com.gradleup.shadow from 9.5.1 to 9.6.1 ([e74bbcd](https://github.com/brbrown25/flink-proto-confluent/commit/e74bbcdb7763bd8420a99bb09baeee117e5d90b2))
* **gradle:** bump gradle-wrapper from 9.6.1 to 9.7.0 ([ca7a0ae](https://github.com/brbrown25/flink-proto-confluent/commit/ca7a0aec05103eea3cacadaa4f36ca2b77bc3057))
* **gradle:** bump gradle-wrapper from 9.7.0 to 9.7.1 ([b0da0ae](https://github.com/brbrown25/flink-proto-confluent/commit/b0da0ae5b844fb3e747e21f384a24f1aa008abd2))
* **gradle:** bump gradle-wrapper from 9.7.1 to 9.8.0 ([2aa5ab7](https://github.com/brbrown25/flink-proto-confluent/commit/2aa5ab77dcdebcd440ae36d8a02225b2ee7e3d69))
* **gradle:** bump org.apache.flink:flink-connector-jdbc ([d673308](https://github.com/brbrown25/flink-proto-confluent/commit/d673308d65f3c4882a74cfaf852d42d02350f389))
* **gradle:** bump the testing group with 2 updates ([09f6b8e](https://github.com/brbrown25/flink-proto-confluent/commit/09f6b8e2ca2d7c39b6fddd5b1aff2fc209a483fa))
* **gradle:** bump the testing group with 2 updates ([a46d7e4](https://github.com/brbrown25/flink-proto-confluent/commit/a46d7e44f704bb5f91c0c0f53a6a284c2d76e150))

## 1.0.0 (2026-07-13)

Initial release. See the [v1.0.0 release notes](https://github.com/brbrown25/flink-proto-confluent/releases/tag/v1.0.0).
