**Notebook, SQL, visualization, and component inventory audit**

Research snapshot: September 25, 2026. This is an architectural recommendation for the Kubernetes platform currently being renamed. It extends the master blueprint and does not describe implemented or benchmarked services. Existing research on compute, security, ingestion, cloud storage, and agents remains authoritative for those integration decisions.

**Recommendation: build one result experience shared by SQL, notebooks, and AI.** Every tabular result should open the same product Result Panel: **Table · Visualize · Profile · Execution**. Users can create several named visualization tabs, change chart type, pick fields and aggregations, apply filters, inspect definitions, duplicate a chart, download permitted output, and add a versioned visualization to a dashboard. This is core product scope, not an optional BI iframe.

Databricks currently documents visualization creation on both SQL results and notebook `display()` outputs, associated result filtering, named tabs, and backend aggregation beyond the displayed row limit. This provides a concrete behavior reference. A lookalike tab that charts only the currently loaded browser rows would miss the important execution behavior. [Databricks visualization workflow](https://docs.databricks.com/aws/en/visualizations/).

**Use mature UI libraries while owning the interaction contract.**

| Component | Initial choice | Reuse versus custom work |
|---|---|---|
| Application shell and design system | React + TypeScript, accessible shared components | Build navigation, tabs, commands, resources, consistent permissions/history/status views, themes, keyboard operation, and accessibility |
| Notebook document/editor foundation | JupyterLab notebook/cell/output components and CodeMirror 6 | Reuse editor/kernel interfaces; build integrated compute attachment, execution controls, runtime metadata, source revisions, inline assistance, collaboration boundaries, and recovery |
| SQL editor | CodeMirror 6 with SQL language support and product metadata completion | Build connection/catalog context, authorized suggestions, parameters, multi-statement results, query history, formatting and error navigation; engine validation remains authoritative |
| Collaborative documents | Qualified Jupyter collaboration/Yjs integrations | Persist and authorize document updates; add comments, presence, source checkpoints, and conflict-aware AI edits; shared text does not imply shared compute credentials |
| Tabular results | AG Grid Community with Infinite Row Model or bounded pages | Build authenticated pagination/filter/sort adapter, type rendering, result limits, column details, copying/export policy, and result-state persistence |
| Native chart renderer | Apache ECharts | Build field picker, chart configuration, shared filters, server aggregation, saved definitions, permissions, export, and dashboard integration |
| Python-produced charts | Jupyter MIME rendering; qualified Plotly/Altair/Matplotlib paths | Bound outputs; isolate active content and disallow ambient application credentials or uncontrolled remote loads |
| Typed result transport | Arrow IPC record batches over authenticated HTTP, with typed JSON fallback | Build schema/result manifests, chunking, cursors, cancellation, backpressure, TTL, authorization, and serialization compatibility |
| Dashboard builder | Native product UI reusing Result Panel/chart definitions | Build layout, filters, parameters, semantic binding, versioned publication, viewer identity and scheduled refresh |
| Semantic definitions | Product-owned versioned catalog assets in PostgreSQL/Git | Define metrics, dimensions, joins, calendars, units, owners, freshness and evaluation examples; compile supported definitions through the governed query service |

JupyterLab publishes reusable components and standalone notebook examples; the proposal does not require embedding the entire JupyterLab application. Its license is BSD 3-Clause. [Component reuse](https://jupyterlab.readthedocs.io/en/stable/extension/components.html), [license](https://github.com/jupyterlab/jupyterlab/blob/main/LICENSE).

CodeMirror's GitHub repositories were archived during a move to the maintainer's Forgejo instance in April 2026; that archival is not evidence that the project ceased development. Track the current upstream and the versions qualified by JupyterLab. Its editor libraries use the MIT license. [Migration announcement](https://discuss.codemirror.net/t/codemirrors-migration-to-forgejo/9706), [current repository](https://code.haverbeke.berlin/codemirror/dev), [license](https://github.com/codemirror/dev/blob/main/LICENSE). Jupyter's collaboration documentation points to its supported collaboration extension; our authorization and persistence design still needs qualification. [Jupyter collaboration](https://jupyterlab.readthedocs.io/en/stable/user/rtc.html), [Yjs](https://docs.yjs.dev/).

AG Grid Community is MIT-licensed. Integrated charting, pivoting, grouped server-side row models, and other Enterprise features have commercial licensing. The Community Infinite Row Model can load a flat result in server-backed blocks; use our own aggregation/chart pipeline. Do not list the full Enterprise feature set as part of a free Community dependency. [AG Grid repository/license](https://github.com/ag-grid/ag-grid), [row models](https://www.ag-grid.com/react-data-grid/row-models/).

ECharts has an Apache-2.0 primary license, with notices for included subcomponents. It can map fields from datasets into series and axes. ECharts is a renderer, not a complete SQL analysis application. Vega-Lite is a good declarative alternative and an optional interchange path for supported charts; do not burden v1 with two competing canonical chart formats. [ECharts license](https://github.com/apache/echarts/blob/master/LICENSE), [datasets and mappings](https://echarts.apache.org/handbook/en/concepts/dataset/), [Vega-Lite grammar](https://vega.github.io/vega-lite/docs/).

**The Result Service is substantial custom backend work.**

Keep a ResultHandle distinct from the query or notebook document. Its manifest binds workspace, creator/effective identity, source execution and revision, statement/cell ID, engine/runtime, schema with stable column IDs, source snapshots where available, creation time, completion state, result limits, policy context, storage location, and expiry. A random result ID is a selector, not authorization.

Schema fidelity matters: retain decimal precision/scale, 64-bit integers, null versus empty string, binary values, date/timezone semantics, arrays/maps/structs, and unsupported-type annotations. Never silently convert all integers to JavaScript Number or decimals to floats. Chart numeric conversions should be explicit and tested; table inspection can preserve exact strings alongside typed metadata. Arrow provides a typed columnar representation and IPC layout, but does not itself supply access policy, asynchronous queries, or a result API. [Arrow format](https://arrow.apache.org/docs/format/Columnar.html), [Arrow JavaScript](https://arrow.apache.org/js/current/).

Return an early bounded preview, then fetch additional pages. Use server-owned chunk manifests or stable cursor mappings over a result snapshot. Paging should not rerun a changing base query with arbitrary OFFSET clauses and pretend all pages came from one execution. Result spooling belongs in scoped encrypted object storage; pagination requests pass through the authorized service. Optional short-lived signed result-chunk URLs must be scoped to the already-authorized artifact and have deliberate revocation/expiry semantics. They are not cloud credentials and must never reveal lake-wide access.

Support statement-level results, query cancellation, queued/starting/running/completed states, warnings, and execution links. Editing SQL after a run should visibly mark the output as belonging to an earlier source revision. Changing a chart should preserve the query unless the user explicitly edits or accepts a proposed SQL change. Databricks' current results pane provides a useful reference for multiple statements, visualizations, filtering and downloads; our limits and policy should be our own measured contract. [SQL results documentation](https://docs.databricks.com/aws/en/sql/user/sql-editor/results).

**Full-data chart aggregation must have explicit meaning.**

There are three distinct cases:

1. **Complete materialized result:** filter and aggregate the complete authorized result snapshot, subject to budgets. A later optimization may use a qualified isolated result-processing engine, but first implement supported transformations with the original query engine or its materialized result mechanism.
2. **Lazy Spark DataFrame:** dispatch the approved aggregation through the existing Spark session/plan with the correct identity. It can require another Spark job. Retain an appropriately scoped server-side handle; do not send an executable plan from an untrusted browser directly to privileged compute.
3. **Truncated preview or sample:** clearly state that the chart/profile covers only that preview/sample. Offer a separate full-data action with its expected compute/scan scope. Never label a calculation over the first page as the full table total.

Preserve SQL semantics when wrapping queries for charting. Multi-statement SQL, temporary tables, non-deterministic functions, engine-specific types, user-defined functions, and stateful notebook variables cannot always be converted into a portable subquery. The adapter must advertise which transformations are supported. When re-execution can observe a new source snapshot, show that the chart is a new execution; do not silently mix it with old tabular results.

Initial chart types should be line, bar, area, scatter, histogram, heatmap, pie/donut, and KPI/table. Add pivot, box plots, funnel/cohort, geospatial maps and richer combinations through tested aggregation profiles. A chart library's ability to draw a box is not evidence that our backend computes correct quartiles at scale. Databricks also documents different aggregation behavior and limits for individual visualization types, reinforcing the need for a per-type capability contract. [Databricks chart types](https://docs.databricks.com/aws/en/visualizations/visualization-types).

Profiles should show schema, null rate, distinct counts, min/max and distributions, with exact/approximate/sample labels. Profile computation is a data query and can be expensive. Bound scan bytes/time, output bytes, chart marks, category cardinality, and distinct/quantile work. Define top-K and “other” aggregation semantics explicitly. Decouple display-page limits, download limits and full-aggregation limits in settings and UI.

**A Spark display SDK belongs in the runtime.**

Provide a small versioned Python SDK with a documented `display(df)` convenience in managed notebook environments. It recognizes supported Spark DataFrames, Pandas/Arrow tables and safe MIME results, registers a ResultHandle, emits result metadata, and connects to the shared panel. Scheduled jobs may emit durable tabular artifacts using the same SDK without an attached interactive browser.

Do not implement Spark display as unrestricted `df.toPandas()` or `collect()`. Spark documents that `toPandas()` loads the data into driver memory and is suitable only for small results. Use bounded previews and server-side aggregation; an unbounded stream needs bounded update windows rather than a pretend finite result. [Spark toPandas documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toPandas.html).

The SDK is our API, not a claim to implement all Databricks `dbutils` or notebook semantics. A compatibility layer can cover specifically tested common functions later. `%sql` should identify whether it executes in the notebook's Spark session or a governed SQL endpoint; temporary views, sessions and authorization can differ. Python and SQL may share Spark session tables under a qualified profile. Scala, R and Python do not automatically share arbitrary variables merely because their cells appear in one notebook. Avoid promising transparent mixed-language execution until each runtime bridge is implemented and tested.

**Security covers results, charts and dashboards as data.**

The query engine applies qualified row/column policy before returning results. The browser cannot choose a stronger principal, access raw lake storage, or authorize itself by presenting an unverified policy version. A saved chart or result cache must not widen access to its underlying result. Default dashboard refresh to the viewer's identity; any publisher/service-principal execution mode is an explicit governed sharing feature.

For result reuse, bind to relevant identity/grants, policy state, input snapshot, query/parameters and runtime behavior, and check current authorization again before retrieval. Invalidate or deny cached access when required. A shared notebook document may be collaborative while output access remains separately controlled. Source sharing does not justify serving Alice's result to Bob when their row policies differ.

Exports use the same authority and include clear preview-versus-complete selection. Large exports should be asynchronous jobs with bounded size, audited destination and retention. PNG/PDF dashboard exports can disclose the same sensitive content as CSV files, so must not evade export policy. When a user has already received plaintext, the product cannot retract it; UI download controls are not a guarantee against copying authorized content.

Treat chart specifications and rich notebook outputs as untrusted. Validate an allowlisted JSON chart schema, bind field IDs to the actual result schema, and prohibit arbitrary JavaScript callbacks or remote data URLs in shared specs. Resolve optional map assets through approved sources with privacy/air-gap support. Sandbox HTML/JavaScript MIME content on an isolated origin with a deliberate content security policy. Assistant-generated visualization specs undergo the same validation as user-created ones.

**The chart definition should be portable and separate from its result.**

Store a small versioned product ChartSpec containing the data reference, source revision, chart kind, field bindings, permitted aggregations, filters, sorting, time grain/timezone, units/formatting, colors, and accessibility description. Compile it to ECharts configuration. Do not persist raw unrestricted ECharts options as the resource's sole contract.

A DashboardVersion owns layout, ChartSpec references, parameter bindings, semantic asset revisions, refresh configuration and permissions. A notebook chart can be published as a reproducible query/table definition or as an explicitly frozen artifact. An arbitrary in-memory DataFrame created by hidden notebook state cannot become a reliable independently refreshing dashboard until its producing pipeline or query is captured. This distinction should be explained by the publishing flow.

Version semantic assets independently from prompts. Each metric needs its grain, aggregation, units, calendar/timezone and permitted joins. Keep executable bindings in the governed query layer. SQL, dashboards, and the data assistant should resolve the same definition; an LLM-written description is not an authoritative calculation. Provide import/adapters for established semantic tools later instead of silently keeping multiple competing registries synchronized in v1.

**Additional inventory gaps to carry into the complete bill of materials.**

| Capability | Recommendation and release scope |
|---|---|
| Result service, chart builder, profiles and dashboards | Core custom modules; use the libraries above |
| Semantic definitions and discovery | Core custom assets/query integration; PostgreSQL metadata/search first, optional external catalog search adapters |
| Data quality / data contracts | Core constrained checks compiled to Spark/SQL before publication, plus persisted results; richer Great Expectations/other rule adapters after compatibility tests |
| Identity and configuration | Core external OIDC support, optional Keycloak, product resource policy, qualified data enforcement, secrets references and inspectable effective settings |
| Agent memory and retrieval | AI module needs an explicit durable store, retrieval policy, retention/deletion and token-budget service; checkpointing, long-term memory and token reduction are separate responsibilities |
| AI gateway | Product provider/auth/capability adapter; allow existing customer gateways; optional external gateway integration does not replace our data-access policy |
| ML experiments/registry | Optional MLflow module; model and agent lineage/traces/evaluations integrate into product resources |
| Feature store | Later optional Feast profile for offline/online feature serving; qualify storage adapters and point-in-time behavior |
| Model serving | Later optional KServe plus qualified model runtimes; vLLM for supported self-hosted LLMs when enabled, with independent model licenses/hardware budgets |
| Data sharing | Later Delta Sharing integration with recipient identity, policy, revocation and table-feature qualification |
| Apps and operational databases | Later isolated app deployment and optional PostgreSQL databases; no implied Lakebase branching or elasticity parity |
| Infrastructure reliability | Core compatibility inventory, migrations, backup/restore, upgrades, metrics/audit/lineage, quotas, image registry integration, admission, secret rotation and installation diagnostics |

Feast provides offline and online feature-store functionality; it is not the product's general ETL scheduler. KServe is a serving control plane and needs compatible runtime/deployment profiles. Delta Sharing is an open sharing protocol and implementation, not automatic equivalence to every managed sharing feature. [Feast](https://docs.feast.dev/), [KServe](https://kserve.github.io/website/docs/intro), [Delta Sharing](https://github.com/delta-io/delta-sharing).

Avoid installing redundant stacks just to list features. A full Superset deployment is not necessary to draw charts in our notebook. A second catalog or query engine must have a defined role and an explicit integration profile. An optional service still needs one owner for its versions, identity, resources and failure handling.

**Useful release tests for this portion of the product.**

- SQL and Spark display produce the same shared Result Panel, typed schema and authorized exports for equivalent inputs.
- A chart over a result larger than the preview computes the full supported aggregation or states that it is sampled/truncated.
- Decimal, long integer, null, timestamp/timezone and nested-type fixtures retain correct values through preview, filters, charting and exports.
- Cancel a chart/profile computation without losing source edits or other results; reconnect after interruption without duplicating execution.
- Two users with distinct row policies cannot read each other's result handles, caches, shared notebook outputs, dashboard snapshots, exported images or AI-created charts.
- Large results do not collect unbounded data into driver or browser memory; backpressure and configurable limits are visible.
- Changed or expired notebook state cannot refresh a chart under a misleading old execution identity.
- Malicious chart config, data URLs and rich MIME output cannot access control-plane tokens or forbidden network destinations.
- A saved dashboard binds reproducible sources and stable versions; “refresh” uses its documented identity and current authorization.
- Keyboard-only and screen-reader operation, reduced motion, source autosave and collaborative AI-patch conflicts pass realistic usability tests.

These are proposed tests, not completed validation. The major implementation cost is the integrated result/query/security contract, not the rendering library.
