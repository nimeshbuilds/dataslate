package platform

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

type Module struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	Category         string   `json:"category"`
	Implementation   string   `json:"implementation"`
	Description      string   `json:"description"`
	Dependencies     []string `json:"dependencies"`
	Modes            []string `json:"modes"`
	EnabledByDefault bool     `json:"enabledByDefault"`
}

func Modules() []Module {
	items := []Module{
		{ID: "postgres", Name: "PostgreSQL", Category: "platform", Implementation: "foundation", Description: "Implemented durable workspace and notebook storage; managed deployment requires a qualified PostgreSQL operator."},
		{ID: "identity", Name: "Identity", Category: "security", Implementation: "foundation", Description: "Implemented OIDC JWT verification and workspace claims; managed identity-provider lifecycle is planned."},
		{ID: "spark-operator", Name: "Spark operator", Category: "compute", Description: "Apache and Kubeflow manifest preview adapters; cluster discovery, submission and reconciliation are planned."},
		{ID: "spark", Name: "Apache Spark", Category: "compute", Dependencies: []string{"spark-operator"}, Description: "Planned isolated interactive and batch Spark runtimes."},
		{ID: "unity-catalog", Name: "Unity Catalog", Category: "governance", Dependencies: []string{"postgres", "identity"}, Description: "Planned catalog and cloud credential integration; full governance qualification remains required."},
		{ID: "delta", Name: "Delta Lake", Category: "storage", Dependencies: []string{"spark", "unity-catalog"}, Description: "Planned Delta table publication and qualified Snappy Parquet ingestion."},
		{ID: "zookeeper", Name: "ZooKeeper", Category: "platform", Description: "Planned coordination for highly available Kyuubi engines."},
		{ID: "ranger", Name: "Apache Ranger", Category: "security", Dependencies: []string{"identity", "postgres"}, Description: "Planned data authorization; row filters and masking require engine-level qualification."},
		{ID: "kyuubi", Name: "Apache Kyuubi", Category: "compute", Dependencies: []string{"spark", "ranger", "zookeeper", "unity-catalog"}, Description: "Planned governed SQL sessions and result retrieval."},
		{ID: "airflow", Name: "Apache Airflow", Category: "engineering", Dependencies: []string{"postgres", "identity"}, Description: "Planned workflows, schedules and native task history integration."},
		{ID: "ingestion", Name: "Batch ingestion", Category: "engineering", Dependencies: []string{"delta"}, Description: "Planned dlt staging and validated Spark publication."},
		{ID: "kafka", Name: "Kafka", Category: "engineering", Description: "Planned existing Kafka integration or managed Strimzi deployment."},
		{ID: "cdc", Name: "Change data capture", Category: "engineering", Dependencies: []string{"kafka", "delta"}, Description: "Planned Debezium connectors and checkpointed CDC application."},
		{ID: "pipelines", Name: "Declarative pipelines", Category: "engineering", Dependencies: []string{"delta", "airflow"}, Description: "Planned Spark declarative pipelines with quality checks and durable runs."},
		{ID: "maintenance", Name: "Table maintenance", Category: "engineering", Dependencies: []string{"delta", "airflow"}, Description: "Planned snapshot-aware optimization, compaction and safe retention."},
		{ID: "lineage", Name: "Lineage", Category: "governance", Dependencies: []string{"unity-catalog"}, Description: "Planned OpenLineage capture and cross-resource lineage views."},
		{ID: "dashboards", Name: "Dashboards and visualization", Category: "analytics", Dependencies: []string{"kyuubi"}, Description: "Planned shared result service, governed chart aggregation and dashboard publication."},
		{ID: "mlflow", Name: "MLflow", Category: "ml", Dependencies: []string{"postgres", "identity"}, Description: "Planned experiments, model registry, traces and evaluations."},
		{ID: "sylon", Name: "Sylon", Category: "ai", Dependencies: []string{"identity"}, Description: "Planned gateway/provider connections, data questions and cell assistance; no model is currently invoked."},
		{ID: "memory", Name: "Agent memory", Category: "ai", Dependencies: []string{"postgres", "identity"}, Description: "Planned permission-aware PostgreSQL/pgvector memory with provenance, expiry and deletion."},
		{ID: "agents", Name: "Agent Studio", Category: "ai", Dependencies: []string{"sylon", "memory", "mlflow"}, Description: "Planned LangGraph execution, reviewed tools and versioned agent publishing."},
		{ID: "headroom", Name: "Headroom", Category: "ai", Dependencies: []string{"sylon"}, Description: "Planned optional context compression; savings and answer quality require workload evaluation."},
		{ID: "model-serving", Name: "Model serving", Category: "ml", Dependencies: []string{"mlflow"}, Description: "Planned KServe and qualified inference runtimes, GPU profiles and endpoint policy."},
		{ID: "feature-store", Name: "Feature store", Category: "ml", Dependencies: []string{"delta", "postgres"}, Description: "Planned Feast integration with point-in-time training data and online features."},
		{ID: "search", Name: "Search and retrieval", Category: "ai", Dependencies: []string{"postgres", "unity-catalog"}, Description: "Planned authorized lexical/vector indexes and synchronization."},
		{ID: "apps", Name: "Application hosting", Category: "applications", Dependencies: []string{"identity", "registry"}, Description: "Planned isolated app deployments with resource grants, authentication and logs."},
		{ID: "sharing", Name: "Data sharing", Category: "collaboration", Dependencies: []string{"delta", "identity"}, Description: "Planned Delta Sharing, recipient lifecycle and governed sharing audit."},
		{ID: "iceberg", Name: "Iceberg profile", Category: "storage", Dependencies: []string{"spark", "identity"}, Description: "Planned separately qualified native Iceberg and Polaris catalog profile."},
		{ID: "federation", Name: "Query federation", Category: "analytics", Dependencies: []string{"identity"}, Description: "Planned Trino connectors, pushdown inspection and governed connection access."},
		{ID: "observability", Name: "Observability", Category: "operations", Description: "Planned OpenTelemetry integration and existing or managed telemetry backends."},
		{ID: "secrets", Name: "Secret management", Category: "security", Description: "Planned External Secrets integration and optional OpenBao; never expose secret values through workspace metadata."},
		{ID: "registry", Name: "Runtime registry", Category: "operations", Description: "Planned existing OCI registry or optional Harbor and isolated BuildKit workers."},
	}
	for i := range items {
		m := &items[i]
		if m.Implementation == "" {
			m.Implementation = "planned"
		}
		if m.Dependencies == nil {
			m.Dependencies = []string{}
		}
		m.Modes = []string{"managed", "external"}
		m.EnabledByDefault = m.ID == "postgres" || m.ID == "identity"
	}
	return items
}

type ModuleChoice struct {
	ID       string `json:"id"`
	Mode     string `json:"mode"`
	Endpoint string `json:"endpoint,omitempty"`
}
type PlanInput struct {
	Namespace string         `json:"namespace"`
	Cloud     string         `json:"cloud"`
	Modules   []ModuleChoice `json:"modules"`
}
type PlanAction struct {
	ModuleID     string `json:"moduleId"`
	Mode         string `json:"mode"`
	Action       string `json:"action"`
	Verification string `json:"verification"`
}
type Plan struct {
	Cloud      string       `json:"cloud"`
	Namespace  string       `json:"namespace"`
	Actions    []PlanAction `json:"actions"`
	Warnings   []string     `json:"warnings"`
	Executable bool         `json:"executable"`
}

func CompilePlan(in PlanInput) (Plan, error) { return compilePlan(in, Modules()) }
func compilePlan(in PlanInput, catalog []Module) (Plan, error) {
	out := Plan{Cloud: in.Cloud, Namespace: in.Namespace, Actions: []PlanAction{}, Warnings: []string{"Preview only: no dependency was installed, contacted or verified.", "Managed/external ownership, versions, credentials and compatibility must be approved before deployment."}}
	if in.Cloud != "aws" {
		return out, errors.New("only the initial aws deployment profile is currently modeled")
	}
	if !dnsLabel.MatchString(in.Namespace) {
		return out, errors.New("namespace must be a Kubernetes DNS label")
	}
	if in.Modules == nil || len(in.Modules) > 64 {
		return out, errors.New("modules must be an array of at most 64 choices")
	}
	defs := map[string]Module{}
	for _, m := range catalog {
		defs[m.ID] = m
	}
	chosen := map[string]ModuleChoice{}
	for _, m := range in.Modules {
		if _, ok := defs[m.ID]; !ok {
			return out, fmt.Errorf("unknown module %q", m.ID)
		}
		if _, ok := chosen[m.ID]; ok {
			return out, fmt.Errorf("duplicate module %q", m.ID)
		}
		if m.Mode != "managed" && m.Mode != "external" {
			return out, errors.New("module mode must be managed or external")
		}
		if m.Endpoint != "" {
			u, err := url.Parse(m.Endpoint)
			if m.Mode != "external" || err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				return out, errors.New("external service endpoints must use HTTPS without credentials, query or fragment")
			}
		}
		chosen[m.ID] = m
	}
	visited := map[string]int{}
	var visit func(string) error
	visit = func(id string) error {
		if visited[id] == 1 {
			return fmt.Errorf("module dependency cycle at %s", id)
		}
		if visited[id] == 2 {
			return nil
		}
		visited[id] = 1
		for _, dep := range defs[id].Dependencies {
			d, ok := defs[dep]
			if !ok {
				return fmt.Errorf("module %s references unknown dependency %s", id, dep)
			}
			if _, ok = chosen[dep]; !ok {
				if d.EnabledByDefault {
					continue
				}
				return fmt.Errorf("module %s requires explicit module %s", id, dep)
			}
			if err := visit(dep); err != nil {
				return err
			}
		}
		visited[id] = 2
		m := chosen[id]
		action := "plan managed installation"
		if m.Mode == "external" {
			action = "plan external integration"
			if strings.TrimSpace(m.Endpoint) == "" {
				out.Warnings = append(out.Warnings, id+": connection coordinates and secret references must be provided separately; no endpoint was verified.")
			}
		}
		out.Actions = append(out.Actions, PlanAction{ModuleID: id, Mode: m.Mode, Action: action, Verification: "required"})
		return nil
	}
	keys := []string{}
	for id := range chosen {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for _, id := range keys {
		if err := visit(id); err != nil {
			return out, err
		}
	}
	return out, nil
}
