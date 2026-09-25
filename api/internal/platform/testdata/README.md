# Pinned upstream CRD schemas

Fixtures are `spec.versions[].schema.openAPIV3Schema` extracted from the official CRDs below, with `description` and `title` documentation removed recursively and JSON whitespace minimized. Types, required fields, validation and Kubernetes extension fields remain unchanged. These Apache-2.0 sources retain their original project copyrights. Fixtures validate JSON structure; they do not run Kubernetes admission, defaulting, CEL or reconciliation, and cannot prove runtime compatibility. Unknown-field pruning remains a live API-server validation gate.

- Apache Spark Kubernetes Operator **1.0.0**, `spark.apache.org/v1`: <https://github.com/apache/spark-kubernetes-operator/blob/1.0.0/build-tools/helm/spark-kubernetes-operator/crds/sparkapplications.spark.apache.org-v1.yaml>
- Kubeflow Spark Operator **2.5.2**, `sparkoperator.k8s.io/v1beta2`: <https://github.com/kubeflow/spark-operator/blob/v2.5.2/config/crd/bases/sparkoperator.k8s.io_sparkapplications.yaml>

Downloaded September 24, 2026. Regenerate only when deliberately changing an adapter's supported CRD version and review schema changes.
