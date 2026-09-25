package platform

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestPreviewsAgainstPinnedUpstreamCRDSchemas(t *testing.T) {
	for _, op := range []string{"apache", "kubeflow"} {
		t.Run(op, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/" + op + "-sparkapplication-schema.json")
			if err != nil {
				t.Fatal(err)
			}
			var doc any
			if err = json.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			c := jsonschema.NewCompiler()
			if err = c.AddResource("schema.json", doc); err != nil {
				t.Fatal(err)
			}
			schema, err := c.Compile("schema.json")
			if err != nil {
				t.Fatal(err)
			}
			in := validSpark()
			in.Operator = op
			out, err := CompileSpark(in, "ws-a")
			if err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(out.Manifest)
			var manifest map[string]any
			json.Unmarshal(encoded, &manifest)
			if err = schema.Validate(manifest); err != nil {
				t.Fatalf("upstream CRD rejected preview: %v", err)
			}
			spec := manifest["spec"].(map[string]any)
			if op == "apache" {
				spec["pyFiles"] = []any{"wrong-type.py"}
			} else {
				spec["executor"].(map[string]any)["instances"] = "wrong-type"
			}
			if err = schema.Validate(manifest); err == nil {
				t.Fatal("schema validation did not reject invalid field type")
			}
		})
	}
}
