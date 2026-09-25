# Kubernetes lakehouse: identity, catalog, governance, and isolation research

Research date: 24 September 2026. Scope: a production platform used by teams within one organization, with separate workspaces, data permissions, and arbitrary user code. This is a researched architecture proposal, not a deployed or penetration-tested implementation. Statements marked as recommendations are design judgments. Component versions below are observations from official sources, not a certified compatibility matrix.

## Executive recommendation

Build the product around a **real enforcement boundary**: governed SQL services read protected data; ordinary analyst notebook containers and AI agents do not receive raw credentials for those tables. Spark engineering workloads run separately with explicitly granted table or storage access. Both experiences appear in the same UI and API, with the effective identity and access mode visible.

Do not promise that installing an open-source catalog automatically gives arbitrary Python, Spark, Trino, and AI identical row-level security. Catalog authorization, Kubernetes authorization, and query-engine filtering protect different things. The combination is a product feature we must engineer, test, and operate.

Given the user's explicit Spark/Databricks/Unity Catalog priorities, the best **primary candidate to qualify** is **UC OSS + Delta + Spark, with Kyuubi and Ranger for the restricted governed Spark SQL service**. This preserves one SQL dialect and avoids a required UniForm/Trino bridge. It is a proposed integrated profile, not one already verified by the upstream projects together.

For the most straightforward documented multi-engine alternative, evaluate **native Iceberg v2 + Apache Polaris + Spark for engineering + Trino with OPA for governed SQL**. Preserve UC as a first-class integration if choosing that profile. Do not register the same writable table independently in two authoritative catalogs. The architectural decision should follow a short, adversarial compatibility spike, before large UI development.

| Candidate | Product fit | Verified upstream pieces | Unresolved integration gate | Recommendation |
|---|---|---|---|---|
| A: UC + Delta + Spark SQL through Kyuubi/Ranger | Strongest match for the requested Databricks-like Spark/Delta experience | Kyuubi Delta connector, Spark AuthZ plugin, generic V2 resource extraction, UC Spark connector | End-to-end UC catalog identity extraction, Ranger resource mapping and masks/filters, scoped backend UC identity, policy refresh, SQL-only escape prevention | Qualify first; use as primary if it passes |
| B: UC + Delta UniForm + Trino/OPA | Keeps UC/Delta with separate fast SQL engine | Official UC Trino read example; Trino OPA enforcement | UniForm write-to-read freshness, table-feature limits, privileged UC Iceberg endpoint, read-only credentials, cache behavior | Optional profile; more bridge complexity than A |
| C: Polaris + native Iceberg + Trino/OPA; Spark engineering | Strong interoperable lakehouse; different default table format | REST catalog and engine integrations, credential vending, Trino policy enforcement | Our complete identity/policy/control-plane integration and production tests still required | Clear fallback or alternate strategic default; retain UC profile |

Source inspection of **Kyuubi v1.12.0** found a real Delta authorization suite using Hive catalog implementation and `DeltaCatalog`, plus a generic V2 relation extractor that handles catalog/identifier metadata. A repository-wide search found no UC-specific connector test or `UCSingleCatalog` reference. Therefore **UC + Kyuubi + Ranger RLS is plausible but not source-verified as a complete combination**. A thin mapping adapter may be necessary. [Kyuubi Delta authorization tests](https://github.com/apache/kyuubi/blob/v1.12.0/extensions/spark/kyuubi-spark-authz/src/test/scala/org/apache/kyuubi/plugin/spark/authz/ranger/DeltaCatalogRangerSparkExtensionSuite.scala), [Kyuubi relation resource extraction](https://github.com/apache/kyuubi/blob/v1.12.0/extensions/spark/kyuubi-spark-authz/src/main/scala/org/apache/kyuubi/plugin/spark/authz/serde/tableExtractors.scala).

## What the candidate catalogs actually provide

| Component | Verified current capabilities | Boundary we must preserve |
|---|---|---|
| Unity Catalog OSS | Open data/AI catalog; external identity authentication and catalog/schema/table privileges; v0.6.0 Spark artifacts for 4.0, 4.1, and 4.2; Delta API and UniForm metadata support in the source | It is not the complete managed Databricks Unity Catalog service. No general managed-UC-equivalent RLS/masking implementation was identified in the checked release |
| Apache Polaris | Iceberg REST catalog; role-based table access; short-lived storage credential vending; external IdP and Kubernetes deployment documentation | Table read credentials expose table files, not only rows permitted by a SQL predicate |
| Lakekeeper OSS | Iceberg REST catalog; OpenFGA authorization, storage delegation, Trino integration | “Fine-grained” table/view grants do not mean universal row filtering. Some attractive capabilities belong to Lakekeeper+ |
| Product governance service | Our proposed common permissions, policies, subject mapping, audit, and policy deployment API | It must compile policies to actual enforcement points and deny unsupported execution paths |

UC OSS authentication documentation separates the external identity provider from its local authorization database. Its v0.6.0 release includes view/metric-view and authentication improvements, with runtime-specific connector artifacts. We inspected that release rather than assuming older 0.3 examples describe its complete current scope. [UC authentication](https://docs.unitycatalog.io/server/auth/), [UC v0.6.0 release](https://github.com/unitycatalog/unitycatalog/releases/tag/v0.6.0).

The v0.6.0 source has SCIM Users and Me endpoints, OAuth token exchange, explicit issuer/audience checks, and privilege APIs. That is useful integration material, but not evidence of full enterprise group provisioning or Databricks governance parity. Implement or qualify provisioning against the actual endpoints. Avoid broad issuer wildcards and disabled audience validation in the product defaults. [Versioned UC authentication documentation](https://github.com/unitycatalog/unitycatalog/blob/v0.6.0/docs/server/auth.md), [UC control API](https://github.com/unitycatalog/unitycatalog/blob/v0.6.0/api/control.yaml).

Managed Databricks ABAC row filters and column masks have specific runtime requirements and query-time behavior. They must not be attributed to the OSS server by name association. [Databricks policy documentation](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/policies).

Polaris 1.7.0 documents separate privileges for reading metadata versus receiving credentials that read/write data. Its policy object documentation includes maintenance policies such as compaction and snapshot expiration; the word “policy” there does not establish row-level security. [Polaris RBAC](https://polaris.apache.org/releases/1.7.0/managing-security/access-control/), [Polaris policy objects](https://polaris.apache.org/releases/1.7.0/policy/).

Lakekeeper is a credible alternative, especially for its OpenFGA relationship model and storage features. Its documentation says vended credentials are downscoped to table locations. Its homepage explicitly places Cedar permission-as-code and enterprise IdP role providers in Lakekeeper+, despite documentation pages describing those features alongside core authorization. Verify the exact distribution, image, license, and API support before including them in a fully open-source bill of materials. [Lakekeeper storage](https://docs.lakekeeper.io/docs/latest/storage/), [Lakekeeper authorization](https://docs.lakekeeper.io/docs/latest/authorization/), [Lakekeeper OSS/Plus capability distinction](https://docs.lakekeeper.io/).

Lakekeeper's view security is instructive: DEFINER views depend on an authenticated **trusted query engine** supplying the view chain. The catalog alone cannot prove that a client followed the SQL definition. This reinforces the proposed trusted query boundary. [Lakekeeper view security](https://docs.lakekeeper.io/docs/latest/view-security/).

## Critical Unity Catalog / Delta / Trino integration qualification

The official UC Trino integration uses the **Trino Iceberg connector** against UC's Iceberg REST endpoint to **read a Delta UniForm table**. It is not a native Trino Delta connector that transparently uses OSS Unity Catalog. [UC Trino integration](https://docs.unitycatalog.io/integrations/unity-catalog-trino/).

UniForm exposes Delta data through Iceberg metadata. Freshness is runtime/path-dependent: Delta 4.3 added atomic/incremental conversion and 4.4 atomic initialization, while other conversion paths remain. Catalog publication and reader caches still require end-to-end qualification. Writers through the Iceberg representation can corrupt the Delta table. Therefore a proposed production connector needs explicit read-only credentials and write denial, an observable converted version, and verification after schema changes, MERGE, DELETE, compaction, and recovery. Separate “committed” from “available to this SQL reader” wherever the qualified path requires it. [Delta UniForm](https://docs.delta.io/delta-uniform/), [updated tagged-release/source findings](research-cloud-storage.md).

Source inspection identified a more consequential issue: in UC **v0.6.0**, `IcebergRestCatalogService` applies **metastore OWNER authorization** to its config/list/load routes, reads registered UniForm metadata, and does not implement a native Iceberg create/commit catalog. Do not expose that token to end-user notebooks or market that route as inheriting users' table permissions. [Versioned UC Iceberg service source](https://github.com/unitycatalog/unitycatalog/blob/v0.6.0/server/src/main/java/io/unitycatalog/server/service/IcebergRestCatalogService.java).

The same release includes a Delta commit path that updates UniForm metadata fields, so it is not appropriate to conclude UC cannot support a continuously updated UniForm integration. It means the whole path must be proven, including the specific Delta client that produces the metadata. [Versioned UC Delta commit implementation](https://github.com/unitycatalog/unitycatalog/blob/v0.6.0/server/src/main/java/io/unitycatalog/server/persist/DeltaCommitRepository.java).

**If UC is the initial default**, a feasible containment design is one UC instance per department/workspace security domain, with its privileged Iceberg token confined to a private SQL service. The query engine enforces product policies under the end user's identity. A read-only network proxy, storage restrictions, separate service account, and private endpoints reduce the token's practical reach; they do not change its UC privilege level. This costs additional catalogs, database schemas, migrations, and cross-workspace sharing machinery. It is a mitigation proposal, not a tested guarantee.

**If interoperability is the initial default**, use a native Iceberg catalog. Trino's Iceberg connector documents REST catalogs and ordinary read/write support. Use v2 tables initially: its current documentation calls v3 support experimental. Add the UC Delta profile without pretending the two formats have identical functionality. [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html).

Do not solve this by creating an unmanaged Hive Metastore mirror and hoping it remains synchronized. A supported metadata bridge would need ownership rules, reliable rename/drop/schema propagation, reconciliation, credential scoping, policy projection, and an explicit freshness SLA. That is substantial product code.

## Concrete security architecture

```text
Organization IdP (or optionally installed Keycloak)
    -> UI / CLI / API authentication
    -> canonical subject + group mapping + workspace membership
    -> product authorization and policy service
         -> permitted project/catalog/connection/job operations
         -> signed, short-lived execution context
         -> Ranger policies or OPA bundle / catalog grants

Governed SQL / dashboards / AI / analyst notebooks
    -> authenticated query gateway
    -> A: Kyuubi -> restricted Spark SQL + Ranger
       or C: Trino + OPA
       table checks + row filters + column masks
    -> catalog: metadata + scoped credentials for trusted engine
    -> object storage

Engineering notebook / scheduled ETL
    -> authorized job specification and run-as identity
    -> isolated Spark driver + executors
    -> catalog credentials for explicitly authorized full tables
    -> approved inputs and outputs

Every boundary -> correlated audit records and policy version
```

These are recommended product interfaces, not components to install unchanged:

1. **One identity contract.** Use immutable `(issuer, subject)` identifiers, canonical group IDs, organization and workspace IDs. Email is an attribute. Never trust tenant ID, group, or run-as identity merely because a client supplied it.
2. **One product permission model.** Separate platform operator, workspace administrator, data steward, engineer, analyst, and service principal. Workspace administration does not automatically grant access to all data, secrets, or another user's query results.
3. **One policy definition API.** Keep policy version, resource identity, subject criteria, effect, row predicate/mask type, permitted execution paths, test fixtures, and deployment state. Compile to the chosen Ranger/OPA backend and catalog grants; do not ask users to maintain three conflicting permission systems.
4. **Fail closed for unsupported execution.** If a table requires row filtering but a requested runtime only implements table credentials, reject the path or use governed result materialization. Never silently downgrade to full-table access.
5. **Delegated execution.** Bind each run to a subject/service principal, workspace, immutable code revision, approved environment digest, and declared data scope. Brokers issue short-lived credentials after authorization. Record who submitted, who approved run-as, and who actually accessed data.
6. **Manage revocation explicitly.** Removing access prevents new queries and new credentials. Define what happens to running jobs, existing temporary credentials, materialized extracts, notebook outputs, and caches. Immediate global revocation cannot be assumed while an already-issued storage token remains valid.

**Two identities at the query boundary are deliberate.** For a protected table, the query engine may need a backend catalog/storage identity with full-table read access, while Ranger or OPA evaluates the authenticated end user's row and column policy. Log both identities and bind them to the same request. Do not give an analyst a UC `SELECT` token or Polaris `TABLE_READ_DATA` credential that can vend raw table access outside that query service. Metadata visible in our catalog UI should come from a permission-filtered product API, rather than granting raw-table privileges simply to populate the browser. Engineering identities with direct full-table access are explicitly different.

For candidate A, block query features that could expose the service identity or bypass the SQL policy path: arbitrary language execution, user-installed functions/JARs, unrestricted configuration inspection or mutation, custom connectors, and raw-file table access. This requires actual server/runtime controls and tests, not just disabling buttons. The same constraints apply to externally connected JDBC/ODBC clients.

Trino has official OPA support for object checks, row filters, and column masks. The row-filter and mask endpoints must be configured; absent endpoints mean those transformations do not apply. Our installer should validate that configuration and run negative tests before allowing protected-table access. OPA supplies decisions; Trino performs query enforcement. [Trino OPA access control](https://trino.io/docs/current/security/opa-access-control.html).

Ranger is the natural first policy backend for candidate A and can also serve a later Trino endpoint. Trino's official Ranger plugin supports row filtering, masking, and audit logging. Choose OPA or Ranger as the product's initial policy backend; deploying both by default creates unnecessary policy consistency work. [Trino Ranger access control](https://trino.io/docs/current/security/ranger-access-control.html).

## Honest enforcement matrix

| Access path | Initial promise | Required boundary |
|---|---|---|
| SQL editor, JDBC/ODBC, dashboard | RBAC, row filters, column masks | Engine-authenticated end-user identity; protected SQL engine; no direct storage credentials to users |
| Mini Genie | Same data policy as the asking user | Every data tool calls governed SQL; permission-filter metadata retrieval; no privileged agent identity |
| Analyst Python notebook | Can use permitted SQL results and ordinary Python libraries | Python container has query credentials only for protected data; own scratch storage; no raw protected table credentials |
| Analyst Spark transformation | Can transform authorized extracts | Materialize an authorized, scoped, expiring result into a separate location, then use Spark on that result; track lineage and snapshot/policy versions |
| Engineer Spark/PySpark/Scala | Table/storage permissions; authorized engineering access | Isolated run identity, explicitly approved raw tables, separate service account; no claim that RLS survives unrestricted direct file reads |
| Restricted Spark SQL service | Potential row filters/masks via Kyuubi/Ranger | No arbitrary Python/Scala/UDF installation, runtime replacement, raw path escape, or mutable security settings; certify connectors |
| Embedded DuckDB | Process engine on already-authorized inputs | It receives extracts, not source storage credentials that bypass policy |
| Source ingestion / CDC | Connector-level source permission, scoped landing writes | Dedicated connector identities, secret controls, destination prefix and schema scope |
| Scheduled privileged pipeline | Explicit service-principal access | Editing code/config/environment for that job requires permission equivalent to its run-as capability |
| Direct object-store access | Object/prefix permissions only | Never label this RLS; privileged users explicitly accept full file access |

The Python/Spark extraction route is intentionally explicit: materialization has cost, latency, expiration, and snapshot semantics. The UI can make it simple—“Use governed data in notebook”—but cannot promise transparent live Spark DataFrame parity until a secure remote execution design is implemented and certified.

Kyuubi 1.12.0 is worth a dedicated spike. Its Spark AuthZ plugin documents column authorization, row filtering, and masking, while explicitly warning that end users can otherwise disable authorization through Spark configuration. This is a good foundation for a **restricted SQL service**, not evidence that arbitrary code sharing the same credentials is safely governed. [Kyuubi 1.12 AuthZ](https://kyuubi.readthedocs.io/en/v1.12.0/security/authorization/spark/overview.html). Its current release notes report Spark 3.3–4.2 verification and include multiple authorization fixes; pin a tested patch release and check advisories. [Kyuubi 1.12 release](https://kyuubi.apache.org/release/1.12.0.html).

## SSO, OAuth, provisioning, and service identity

Support external OIDC first, with optional managed Keycloak. The same organization normally needs one identity boundary with workspace membership inside the product; a realm per workspace should be a deliberate exception. Browser sessions use secure server-side session handling and the authorization-code flow with PKCE. CLI uses device or browser-based login; automation uses dedicated service principals and narrowly scoped credentials. Restrict issuer, audience, signing algorithms, and redirect URLs.

Keycloak supplies identity brokering, groups, roles, and standards-based login. Its current administration guide marks the SCIM API **Preview and disabled by default**, so do not make production provisioning depend blindly on it. First integrate the organization's IdP and implement/qualify the product's lifecycle provisioning endpoint or directory sync separately. [Keycloak administration guide](https://www.keycloak.org/docs/latest/server_admin/index.html#_scim).

Keycloak's standard token exchange can support audience-specific service calls, but the platform must configure allowed exchanges and effective scopes; exchanging a token does not itself enforce a data policy. Avoid accepting user-controlled impersonation headers between services. [Keycloak token exchange](https://www.keycloak.org/securing-apps/token-exchange).

The secrets model must distinguish **can use** from **can reveal**. A user permitted to execute arbitrary code with a secret can usually exfiltrate it through that code. Hide credentials in the UI for convenience, but use credential brokers and connector boundaries when the user must never receive the credential. Never store secrets in notebook files, Spark configuration visible in logs, table properties, or AI prompts.

## Kubernetes isolation for the first organizational deployment

Teams in one organization can share a cluster while retaining useful isolation. Kubernetes itself describes namespaces as a mechanism that needs additional network, access, and security controls. Shared containers still share a host kernel. Dedicated nodes or clusters remain an escalation option for especially sensitive departments, not a requirement for every initial deployment. [Kubernetes multi-tenancy](https://kubernetes.io/docs/concepts/security/multi-tenancy/).

Recommended defaults:

- Separate the control plane from user workloads. Do not schedule arbitrary notebooks on nodes holding identity, catalog, control database, and signing services when a separate pool is available.
- Workspace namespaces for trusted collaboration; dedicated execution namespaces or admission-constrained identities when users inside a workspace must not borrow one another's privileges.
- Enforce namespace quotas, LimitRanges, bounded ephemeral storage, priority rules, and job concurrency so one team cannot exhaust cluster capacity.
- Default-deny ingress/egress policies with required DNS, query service, catalog, storage, package mirror, and approved source-system exceptions. Validate the chosen CNI actually enforces them.
- Restricted Pod Security, non-root execution, dropped capabilities, seccomp, read-only root filesystem where supported, no hostPath/hostPID/hostNetwork, and signed/approved runtime images.
- Do not give users direct pod-create privileges to bypass the product's environment and credential restrictions. A job request is a constrained API object, not arbitrary Kubernetes YAML.
- Spark drivers get only the Kubernetes privileges they need to launch executors. Executors and notebook kernels should not automatically receive Kubernetes API credentials.
- Admission checks must prevent user-controlled service accounts, secret mounts, images, privileged containers, and network-policy mutations. Namespace RBAC alone cannot constrain every field of a newly created pod.
- TLS to browsers and API clients, protected engine endpoints, certificate rotation, and authenticated service-to-service access. Spark security features require explicit configuration; defaults are not the production security profile. [Spark security](https://spark.apache.org/docs/latest/security.html).

The pod-create issue is especially important: Kubernetes warns that workload creation can grant access to namespace secrets and service-account permissions. A broadly privileged Spark driver in a namespace containing other identities is therefore a privilege-escalation path unless admission or namespace boundaries close it. [Kubernetes RBAC good practices](https://kubernetes.io/docs/concepts/security/rbac-good-practices/).

## AI-specific governance and UI obligations

The assistant's tools should be `search_authorized_catalog`, `explain_metric`, `validate_query`, `run_query`, `read_authorized_result`, and optionally `propose_pipeline_change`. The backend chooses identity and scope; the model cannot set an administrator identity or invent a broader connection. Provider API keys are stored through the secrets system. Data allowed into prompts must be separately configurable because authorization to view a result is not necessarily authorization to send it to an external provider.

Do not build one organization-wide embedding index containing private schemas, sample rows, notebook outputs, and query results and rely on a prompt to keep them apart. Retrieval filters and indexes must enforce permissions, including revocation. Result caches must include effective policy and identity scope or be disabled for protected data. Dashboard refresh and scheduled emails/exports need explicit viewer-versus-owner identity semantics.

The intuitive product opportunity is a **permission explanation UI**: “You can query this table through the warehouse, with region filtering and email masking. Direct file access is unavailable. This job runs as finance-daily-etl.” Show who can change each policy, test it with synthetic identities, preview masked schemas, and explain why an execution path was denied. Users should not have to debug OPA, cloud IAM, and Spark service accounts independently.

## Production acceptance gates

These are recommended tests and acceptance criteria, not assertions that upstream projects already meet them together:

1. **Cross-workspace negative tests:** API object IDs, query results, logs, notebook outputs, catalog discovery, storage paths, and AI retrieval cannot cross unauthorized boundaries.
2. **RLS/masking correctness:** base tables, joins, subqueries, views, aggregate queries, metadata tables, snapshots/time travel, error messages, caches, exports, and prepared statements preserve policies or fail closed.
3. **Escape tests:** protected data cannot be read via raw S3 URLs, filesystem readers, direct catalog tokens, changed Spark settings, arbitrary UDFs, alternate engines, or catalog registration procedures.
4. **Run-as escalation tests:** editing a privileged job, DAG, notebook dependency, container image, or secret reference is restricted; read-only users cannot turn a permitted run into arbitrary privileged code.
5. **Lifecycle tests:** user removal, group changes, policy edits, expired credentials, stopped jobs, and disabled connectors have documented effects on active sessions and retained outputs.
6. **Failure tests:** identity provider, catalog, OPA, or secret service outage fails closed for protected new access while preserving comprehensible job states; retry never broadens permissions.
7. **Audit completeness:** user -> API request -> policy version -> query/run -> engine attempt -> catalog/storage credential event -> output is traceable without logging secret values or unnecessary result data.
8. **Upgrade tests:** schema migrations, permission models, signed runtime changes, and controller upgrades preserve grants and denials; rollback and restore are exercised.
9. **UC/Kyuubi profile gate:** test fully qualified catalog names, duplicate schema/table names across catalogs, UC token renewal, grants/revocation, filter/mask application before user-visible results, all enabled Delta operations, alternate relation types, view resolution, paths, and rejection of non-SQL code/configuration escape. Fail if any table resolves without the correct Ranger resource identity.
10. **UC/UniForm profile gate:** automated proof that Spark commits become visible through the governed SQL path with correct data, metadata version, authorization, and read-only semantics after failure/retry.

The first security release should explicitly promise organizational workspace isolation, scoped raw-table engineering, governed SQL/AI row and column controls, secret management, and auditable identity propagation. Unrestricted Spark with universal fine-grained policy enforcement is a separate, substantial engineering program; a UI policy toggle cannot provide it.
