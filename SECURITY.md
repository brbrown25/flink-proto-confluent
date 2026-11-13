# Security Policy

## Reporting a vulnerability

Please report vulnerabilities privately through [GitHub's private vulnerability reporting](https://github.com/brbrown25/flink-proto-confluent/security/advisories/new), not as a public issue, pull request or discussion.

Include:

- the affected version, and which JAR you use (the library JAR or the self-contained `-all` JAR),
- your Flink, Confluent Platform and protobuf-java versions,
- how to reproduce it, ideally with the table DDL and a protobuf schema or record that triggers it,
- what an attacker gains: for example reading data they should not, crashing or stalling a job with a crafted record, or reaching a Schema Registry or Kafka cluster with someone else's credentials.

You should get an acknowledgement within a week. This is a small project maintained in spare time, so there is no guaranteed fix timeline, but confirmed reports are prioritised over other work and you will be kept informed through the advisory.

Please do not report a vulnerability you have not reproduced. A model's or scanner's reading of the code is not a report; see the [AI usage policy](AI_POLICY.md).

## Supported versions

Security fixes go into the next release from `main`. Only the latest release is supported; there is no commitment to patch earlier ones.

## Scope

In scope: the code in this repository and the artifacts published from it (`com.bbrownsound:flink-proto-confluent` on Maven Central, including the `-all` JAR and what it bundles).

Vulnerabilities in a bundled dependency (protobuf-java, the Confluent Schema Registry client, kafka-clients and others listed in the [classpath reference](docs/content/reference/classpath.md)) are in scope when this project needs to act, typically by releasing a build with a fixed version. If the flaw is in the dependency itself, please also report it to that project.

Out of scope: Apache Flink, Kafka and Schema Registry themselves, and misconfiguration of a deployment, such as a Schema Registry exposed without authentication.
