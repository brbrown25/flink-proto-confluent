# SSL and authentication

All registry client settings are exposed as typed options under the format prefix.

=== "TLS"

    ```sql
    'value.proto-confluent.url' = 'https://schema-registry:8443',
    'value.proto-confluent.ssl.truststore.location' = '/etc/flink/truststore.jks',
    'value.proto-confluent.ssl.truststore.password' = '...'
    ```

=== "Mutual TLS"

    ```sql
    'value.proto-confluent.url' = 'https://schema-registry:8443',
    'value.proto-confluent.ssl.truststore.location' = '/etc/flink/truststore.jks',
    'value.proto-confluent.ssl.truststore.password' = '...',
    'value.proto-confluent.ssl.keystore.location' = '/etc/flink/keystore.jks',
    'value.proto-confluent.ssl.keystore.password' = '...'
    ```

=== "Basic auth"

    ```sql
    'value.proto-confluent.basic-auth.credentials-source' = 'USER_INFO',
    'value.proto-confluent.basic-auth.user-info' = 'user:password'
    ```

=== "Bearer token"

    ```sql
    'value.proto-confluent.bearer-auth.credentials-source' = 'STATIC_TOKEN',
    'value.proto-confluent.bearer-auth.token' = '...'
    ```

Any other Schema Registry client property can be passed through `properties` (for example `'value.proto-confluent.properties' = 'key:value'`). Typed options win over the same setting tunneled through `properties`; see [precedence](../reference/configuration.md#option-precedence-and-validation).

Each source and sink closes its Schema Registry client, and with it whatever its credential provider holds (an OAuth token provider's threads and token cache, for example), when the job leaves the TaskManager (finished, cancelled or failed for good), through a release hook on the job's user-code class loader. The catalog closes its client when the catalog is closed.

!!! tip
    Keystore and truststore paths are read on the Flink task managers, so the files must exist on every node that runs the job.

## Where credentials are visible

The options above are ordinary Flink table options, so the passwords, `basic-auth.user-info` and `bearer-auth.token` are stored and shown with the rest of the table:

- **Table definitions.** `SHOW CREATE TABLE` prints every option of a table, credentials included, to anyone who can use the table. Flink masks options whose names contain `password`, `token`, `basic-auth` and similar in its error messages and logs, but not in the table definition.
- **Catalog tables.** The [Schema Registry catalog](catalog.md) copies its connection options, credentials included, into every table it returns, so anyone who can use the catalog can read them with `SHOW CREATE TABLE`. Give the catalog credentials that only read the registry.
- **The registry URL.** User info in the URL (`https://user:password@schema-registry:8443`) is part of the `url` option like any other credential. The format strips it from the URL before logging it, so it does not appear in the task manager logs.
- **The `ddl` command.** Credentials passed as arguments (`--basic-auth.user-info user:password`) are visible in `ps`, shell history and CI logs. Each credential option also has an `-env` and a `-file` variant that reads it from an environment variable or a file instead; see [Registry authentication and TLS](generate-ddl.md#registry-authentication-and-tls).
