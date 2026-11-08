# Approved Spark compute namespace boundary

This optional profile renders administrator-reviewed resources for **one approved workload identity in one dedicated compute namespace**. It does not install an operator, apply resources, attach an IAM role automatically, or certify arbitrary notebook code as safe. The initial target is Kubernetes 1.34 or later with native `ValidatingAdmissionPolicy`, Pod Security Admission and a CNI that enforces NetworkPolicy. Cloud IAM and real EKS/S3 qualification remain separate gates.

## Render and review

Copy `example.json`, replace the placeholder image digest and every cluster-specific network/identity value, then render:

```sh
python3 deploy/compute/generate.py --config /path/to/reviewed-profile.json --output /path/to/compute-boundary.json
python3 -m unittest discover -s deploy/compute -p 'test_*.py' -v
kubectl --context YOUR_EXPLICIT_TEST_CONTEXT apply --dry-run=server -f /path/to/compute-boundary.json
```

The generator uses only Python's standard library, rejects unknown fields, never invokes `kubectl`, and refuses to overwrite an output file. Generated output is a Kubernetes JSON `List`, accepted directly by `kubectl`. The example digest, CIDR and role identities are placeholders. A server dry run validates resources but **does not prove an admission policy enforces requests**: the policy and its binding must exist in a disposable namespace for positive and negative Pod probes.

After the policy and binding have been applied in an isolated test namespace, run reproducible read-only/server-dry-run checks:

```sh
python3 deploy/compute/verify_admission.py --context YOUR_EXPLICIT_TEST_CONTEXT --config /path/to/reviewed-profile.json
```

This verifier never creates live Pods. It requires permission to inspect the policy and impersonate the configured operator/driver service accounts; permission or transport failures do not count as successful negative tests. It tests 15 allow/deny cases, but not the separate role-update, network or Spark execution gates.

A cluster administrator must separately review and apply the resources. Cluster-scoped admission policy names include the namespace. Do not place unrelated workloads, secrets, service accounts or additional privileged controllers in this namespace. An existing namespace is not automatically inspected/adopted: review its live contents, roles, admission exceptions and additive network policies first. The default service account token automount is disabled. Retain compute namespace access until all DataSlate runs have drained; uninstalling DataSlate does not stop Spark applications automatically.

## Resources and identity boundaries

The output includes a namespace enforcing the Kubernetes `restricted` Pod Security Standard pinned to `v1.34`, one Spark service account, namespaced driver Role/RoleBinding, quota/LimitRange, default-deny ingress/egress, explicit allow policies, and a native fail-closed admission policy/binding. The binding selects the immutable namespace-name label; changing Pod labels cannot opt out of admission. [Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/), [Validating Admission Policy](https://kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/)

The Spark driver needs to create and observe executor Pods and supporting Services/ConfigMaps. Its role permits the relevant namespaced get/list/watch/create/delete operations, Pod patch, and collection deletion for cleanup. It has no Secrets, RBAC, service-account token issuance, impersonation, PVC or cluster-scoped permissions. PVC cleanup may therefore emit an authorization warning in the pinned Spark runtime; PVC-backed execution is unsupported in this profile. [Spark 4.1.3 Kubernetes prerequisites and RBAC](https://spark.apache.org/docs/4.1.3/running-on-kubernetes.html#rbac)

Admission additionally requires:

- The configured service account and exact digest allowlist for every regular/init container; no ephemeral debug containers or Pod image-pull Secret references.
- Only the configured operator identity may create driver Pods. That identity or the approved driver identity may create executor Pods. Spark role labels cannot change on update.
- Executors disable the default Kubernetes API token automount. With IRSA explicitly enabled, executors may receive only STS-audience projected tokens, never the default Kubernetes API audience.
- No Secret environment references or Secret/hostPath/PVC/CSI/NFS volumes. Only `emptyDir`, ConfigMap, downward API and restricted projected identity sources are admitted. Host networking, PID and IPC sharing are prohibited; PSA supplies the remaining restricted-container checks.

This does **not** isolate malicious code within a workload identity. A driver already owns its approved token and can read/modify/delete that identity's namespace resources. It can pass its own credentials to subprocesses or executors. Namespace RBAC cannot limit create permissions to dynamically generated run names. All runs sharing this namespace therefore share a trust and data-access boundary; different privileges require different namespaces/service accounts and admission profiles. Spark RPC between applications is also permitted within the namespace on the pinned ports; per-run authenticated/encrypted communication is a separate runtime qualification.

## Required runtime settings

The admission/network profile intentionally rejects incompatible Spark defaults. DataSlate's approved application manifest must set these fixed Spark properties:

```json
{
  "spark.driver.port": "7078",
  "spark.blockManager.port": "7079",
  "spark.kubernetes.authenticate.driver.serviceAccountName": "YOUR_APPROVED_SERVICE_ACCOUNT",
  "spark.kubernetes.authenticate.executor.serviceAccountName": "YOUR_APPROVED_SERVICE_ACCOUNT",
  "spark.kubernetes.executor.podTemplateContainerName": "executor"
}
```

Both Apache operator `driverSpec.podTemplateSpec.spec` and `executorSpec.podTemplateSpec.spec` must include:

```json
{
  "securityContext": {
    "runAsNonRoot": true,
    "runAsUser": 185,
    "runAsGroup": 185,
    "seccompProfile": {"type": "RuntimeDefault"}
  },
  "containers": [{
    "name": "spark-kubernetes-driver",
    "securityContext": {
      "allowPrivilegeEscalation": false,
      "capabilities": {"drop": ["ALL"]}
    }
  }]
}
```

Use container name `executor` in the executor template and set its Pod-level `automountServiceAccountToken:false`. The current candidate pins numeric UID/GID 185, verified against the selected official Spark image. The approved image must be compatible with that identity and the restricted settings; custom images using another identity need a separately implemented and qualified runtime profile. An image whose OCI user is a name such as `spark` needs this numeric override for kubelet to verify `runAsNonRoot`. Template merge behavior must be verified against the actual Apache operator release; a valid CRD schema alone is insufficient. No arbitrary user Pod template is accepted by the execution API.

The LimitRange supplies bounded defaults and caps each container at 4 CPU/8 GiB/10 GiB ephemeral storage. ResourceQuota caps aggregate requests/limits, Pod count and supporting objects; PVC and Secret quotas are zero. This is a bounded batch profile. Large executors, private registry pull Secrets, sidecars, GPU workloads, disk/PVC shuffle, dynamic resource allocation and arbitrary session workloads need separately designed profiles, not relaxed exceptions hidden in this one.

## Network and cloud configuration

`apiCidrs` and `apiPort` are explicit Kubernetes API destinations, allowed only to driver-labeled Pods. Discover the actual endpoint addresses and test the CNI's pre/post-Service-DNAT behavior; the Service ClusterIP and endpoint IP/port may both need rules, which this single-port profile cannot represent simultaneously. Node-local DNS is not supported by the namespace/Pod DNS selector; use a separately reviewed profile if the cluster uses a link-local DNS cache.

Only DNS to the selected namespace/Pods, Spark TCP 7078/7079 within the compute namespace, driver API access and explicitly configured S3/STS HTTPS destinations are permitted. Empty S3/STS CIDR arrays deny those paths. No blanket internet egress, link-local address ranges or wildcard CIDRs are emitted. The generator also rejects any range overlapping AWS IPv6 metadata `fd00:ec2::254` or Pod Identity `fd00:ec2::23`. [EC2 metadata endpoints](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html), [EKS Pod Identity agent endpoints](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html) Cloud public IP ranges change: prefer customer-controlled VPC endpoint addresses where practical, and independently maintain any approved ranges. Standard NetworkPolicy filters IP/port, not bucket names or HTTP requests. Enforcement depends on the installed CNI and is additive with other policies. [Kubernetes NetworkPolicy semantics and limitations](https://kubernetes.io/docs/concepts/services-networking/network-policies/)

For optional IRSA, set an exact `irsaRoleArn` and matching explicit `stsCidrs`. The generator adds the service-account annotation and permits STS-audience projected tokens. The cluster's trusted identity webhook must inject the appropriate volumes/environment; the image's AWS client must support the provider. Provision the IAM trust for this precise namespace/service account and restrict its permissions to the intended S3 bucket/prefix and KMS key. That trust/policy is not created here. NetworkPolicy cannot enforce S3 prefixes. EKS Pod Identity's link-local agent endpoint is intentionally unsupported by this profile. Registry access occurs from the node and is not controlled by these Pod egress rules. [Amazon EKS IRSA setup](https://docs.aws.amazon.com/eks/latest/userguide/associate-service-account-role.html)

## Validation and release status

An isolated local k3s `v1.35.0+k3s1` qualification with Apache operator `1.0.0`, chart `1.8.0` and Spark `4.1.3` passed the 15 admission probes with no CEL warnings, and then ran the approved Python fixture under this boundary through successful completion, observed application failure, cancellation and grant revocation. Observed executor Pods used UID/GID 185 and lacked Kubernetes API token mounts. These local results do not qualify EKS, S3/IRSA, production sizing, OIDC or durable recovery as one integrated cloud profile.

Local Python tests cover strict configuration parsing, immutable images, denied broad/metadata egress, least-privilege resource structure, resource caps and the absence of a Pod-label admission bypass. They are **not a substitute for executing CEL in Kubernetes**.

Qualification must apply the policy in a disposable namespace, require no policy type-check warnings, admit a legitimate driver/executor, and reject alternate service accounts, tagged/unapproved images, Secret volumes/env references, hostPath/host namespaces, executor API-token projections, unauthorized driver creation and Pod role changes. Then run a real Spark application under the policy, verify network denial and allowed S3 access, exercise failure/cancellation, and confirm namespace cleanup. Preserve the actual evidence and version matrix; passing API-schema or Pod dry-run checks alone does not qualify Spark execution, network isolation or EKS IAM.

Admission applies to future matching create/update requests, not to already running Pods. Kubernetes administrators, admission-policy owners and trusted operators can change or bypass these controls; their permissions and audit must be governed separately. Availability of a policy object alone is not proof that its binding is active, CEL expressions are healthy, or platform-level admission exemptions are absent.

## Optional saved Python notebook source protection

For `executionKind: "notebook-python-batch"`, add this explicit controller identity to the profile JSON before rendering/applying it:

```json
"notebookSources": {
  "controllerNamespace": "dataslate",
  "controllerServiceAccount": "dataslate-controller"
}
```

Use the actual Helm release's controller service account in its control-plane namespace. It must differ from the compute namespace and may not be the default service account. Omission preserves the approved-application-only profile; do not enable notebook bindings without this additional protection.

The generator adds a fail-closed native ConfigMap admission policy reserving names matching `^ds-[a-f0-9]{32}-source$` for that exact controller identity on CREATE, UPDATE and DELETE. It checks both new and old objects regardless of caller-supplied labels, so removing labels cannot bypass protection. Drivers retain ordinary Spark ConfigMap access but lose ConfigMap `deletecollection`; collection deletion cannot be permitted to bypass the source reservation. The controller receives get/create/patch/delete from the platform chart, and checks exact immutable bytes, labels, hash, UID and resourceVersion. The API has no direct ConfigMap privileges.

Immutable data alone does not prevent deletion/replacement. The admission policy closes that path for workload identities. Cluster administrators/admission owners can still change the policy, and drivers can still read other sources within their engineering namespace. Use one intended trust domain and data privilege boundary per compute namespace; this does not turn arbitrary Python into a hostile multiuser sandbox or an S3 row-policy engine.

The source's SparkApplication owner reference is for verified ownership; automatic garbage-collector deletion is intentionally not broadly authorized. `blockOwnerDeletion:false` allows foreground Spark cancellation, and the DataSlate controller explicitly cleans up source artifacts after terminal execution. Keep this exact controller identity, source admission policy and namespace RBAC through draining, including unresolved terminal cleanup. A late/uncertain source create without an observed UID remains pending rather than claiming an absent artifact has been cleaned. Those runs continue to count against workspace capacity.

With this option, `verify_admission.py` checks the exact source policy/controller rules, active binding, observed generation and absence of CEL warnings or selector bypasses. It adds three server-side source CREATE probes, for 18 dry-run checks total. It creates no live resource and explicitly excludes source UPDATE/DELETE runtime probes. The separate `scripts/local-k8s-qualification.sources.py` is an explicit disposable-local-cluster test: it creates one empty immutable source ConfigMap, tests UPDATE/DELETE/immutability using elevated identity impersonation to isolate admission from RBAC, and removes the fixture in `finally`. The real notebook harness independently exercises minimally privileged service-account tokens.

The local notebook qualification passed all 18 generic checks, eight source mutation probes and 11 RBAC checks, then executed real saved Python success/failure/cancel/deadline cases. Source ConfigMaps were absent after each confirmed cleanup. The successful Spark calculation used the accepted revision even after the original notebook was edited and deleted. Cloud data access, production OIDC/PostgreSQL integration and comprehensive hostile-code testing remain outside that local evidence.
