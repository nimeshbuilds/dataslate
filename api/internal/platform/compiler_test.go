package platform

import (
	"encoding/json"
	"strings"
	"testing"
)

func validSpark() SparkInput {
	return SparkInput{Name: "daily-load", Operator: "apache", Image: "registry.example/spark:4.1.3", MainApplicationFile: "s3://lake/jobs/main.py", ServiceAccount: "spark-writer", Namespace: "team-a", ExecutorInstances: 2}
}
func TestSparkOperatorSchemas(t *testing.T) {
	for _, op := range []string{"apache", "kubeflow"} {
		in := validSpark()
		in.Operator = op
		out, err := CompileSpark(in, "ws-a")
		if err != nil {
			t.Fatal(err)
		}
		if out.Submitted {
			t.Fatal("preview reported submission")
		}
		spec := out.Manifest["spec"].(map[string]any)
		if op == "apache" {
			if out.Manifest["apiVersion"] != "spark.apache.org/v1" || spec["pyFiles"] != in.MainApplicationFile || spec["mainApplicationFile"] != nil || spec["type"] != nil {
				t.Fatal("mixed Apache and Kubeflow schemas")
			}
			conf := spec["sparkConf"].(map[string]string)
			if conf["spark.kubernetes.authenticate.driver.serviceAccountName"] != in.ServiceAccount || conf["spark.executor.instances"] != "2" {
				t.Fatal(conf)
			}
		} else {
			if out.Manifest["apiVersion"] != "sparkoperator.k8s.io/v1beta2" || spec["mainApplicationFile"] != in.MainApplicationFile || spec["pyFiles"] != nil || spec["type"] != "Python" {
				t.Fatal("mixed Kubeflow and Apache schemas")
			}
		}
		b, _ := json.Marshal(out)
		if !strings.Contains(string(b), `"submitted":false`) {
			t.Fatal("missing explicit submission state")
		}
	}
}
func TestSparkInvalidInputs(t *testing.T) {
	for _, mutate := range []func(*SparkInput){func(s *SparkInput) { s.Namespace = "../../admin" }, func(s *SparkInput) { s.ServiceAccount = "" }, func(s *SparkInput) { s.Image = "spark:latest" }, func(s *SparkInput) { s.Image = "spark" }, func(s *SparkInput) { s.Image = "x; curl evil" }, func(s *SparkInput) { s.MainApplicationFile = "https://user:secret@example/main.py" }, func(s *SparkInput) { s.MainApplicationFile = "local:///opt/../secret.py" }, func(s *SparkInput) { s.MainApplicationFile = "s3://lake/first.py,s3://other/second.py" }, func(s *SparkInput) { s.MainApplicationFile = "s3://lake/main.jar" }, func(s *SparkInput) { s.ExecutorInstances = 0 }, func(s *SparkInput) { s.ExecutorInstances = 101 }, func(s *SparkInput) { s.Operator = "unknown" }} {
		in := validSpark()
		mutate(&in)
		if _, err := CompileSpark(in, "ws-a"); err == nil {
			t.Fatalf("accepted invalid input %+v", in)
		}
	}
}
func TestPlanDependenciesModesAndCycles(t *testing.T) {
	in := PlanInput{Cloud: "aws", Namespace: "dataslate", Modules: []ModuleChoice{{ID: "spark", Mode: "managed"}, {ID: "spark-operator", Mode: "external", Endpoint: "https://operator.example"}}}
	p, err := CompilePlan(in)
	if err != nil {
		t.Fatal(err)
	}
	if p.Executable || len(p.Actions) != 2 || p.Actions[0].ModuleID != "spark-operator" {
		t.Fatal("plan not topological/non-executable")
	}
	for _, mods := range [][]ModuleChoice{{{ID: "spark", Mode: "managed"}}, {{ID: "spark", Mode: "managed"}, {ID: "spark", Mode: "external"}}, {{ID: "unknown", Mode: "managed"}}, {{ID: "kafka", Mode: "external", Endpoint: "http://kafka.example"}}, {{ID: "kafka", Mode: "external", Endpoint: "https://user:password@kafka.example"}}, {{ID: "kafka", Mode: "automatic"}}} {
		in.Modules = mods
		if _, err := CompilePlan(in); err == nil {
			t.Fatalf("invalid plan accepted: %+v", mods)
		}
	}
	in.Modules = []ModuleChoice{{ID: "a", Mode: "managed"}, {ID: "b", Mode: "managed"}}
	_, err = compilePlan(in, []Module{{ID: "a", Dependencies: []string{"b"}}, {ID: "b", Dependencies: []string{"a"}}})
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatal("dependency cycle not rejected")
	}
}
func TestCatalogBroadScopeAndNoFalseImplementation(t *testing.T) {
	items := Modules()
	seen := map[string]bool{}
	for _, m := range items {
		if seen[m.ID] {
			t.Fatal("duplicate module")
		}
		seen[m.ID] = true
		if m.Implementation != "foundation" && m.Implementation != "planned" {
			t.Fatal("invalid status")
		}
	}
	for _, id := range []string{"agents", "memory", "headroom", "feature-store", "model-serving", "sharing", "apps", "dashboards", "federation", "iceberg"} {
		if !seen[id] {
			t.Fatalf("broad capability missing: %s", id)
		}
	}
}
