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
