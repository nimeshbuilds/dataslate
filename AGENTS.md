# DataSlate development contract

Product name: DataSlate. Assistant: Sylon. Kubernetes is the deployment target; EKS/S3 is the first qualification profile. Teams share one organizational installation. The product is open source with paid support or hosting as a distribution direction.

Read `docs/implementation/foundation-contract.md` before editing contracts. Consult `docs/implementation/coverage.md` for the full scope. Broad BI, ML, agents, apps and sharing capabilities are in scope, with status tracked individually.

- Never manufacture successful backend operations or hide unsupported capabilities behind fake results. Distinguish preview, implementation, automated tests and live qualification.
- API, UI and SDK use the same resources, identity, policy and revision rules. No independent browser-only persisted copy of server-owned resources.
- Apply workspace authorization to every resource access, including outputs, traces, memory, logs and exports. Shared notebook documents must not carry restricted execution outputs.
- Production auth fails closed; no implicit development mode, unverified user headers or broad credential fallback. Do not log tokens or store secret values in project resources.
- External dependencies remain externally owned. Managed provisioning is explicit. Do not silently replace cluster-scoped CRDs, operators or storage.
- Keep one scheduler and one whole-job retry owner. Reconcile unknown side effects before retrying.
- Pin runtime/profile combinations and verify current upstream interfaces. Independent version support does not certify the integrated system.
- Run meaningful tests for modified boundaries and document live checks not run. Preserve existing user changes. No remote publishing or cluster mutation is implied by local implementation work.
