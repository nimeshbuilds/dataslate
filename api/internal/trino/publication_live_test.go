package trino

import (
	"context"
	"os"
	"testing"
)

// Explicit local fixture only. It reads an already registered synthetic table;
// it never creates, alters or drops a catalog mapping. This does not qualify S3.
func TestLiveDeltaPublicationReadback(t *testing.T) {
	if os.Getenv("DATASLATE_TEST_DELTA_LOCAL_READBACK") != "1" {
		t.Skip("requires the explicitly provisioned synthetic lake fixture")
	}
	client, err := New(Config{Endpoint: os.Getenv("DATASLATE_TEST_DELTA_ENDPOINT"), CAFile: os.Getenv("DATASLATE_TEST_DELTA_CA"), TokenFile: os.Getenv("DATASLATE_TEST_DELTA_TOKEN")})
	if err != nil {
		t.Fatal(err)
	}
	target := PublishTarget{Catalog: "lake", Schema: "default", Table: "orders", Location: "file:///lake/orders/", Version: "0", MinReaderVersion: 1, MinWriterVersion: 2, AllowLocalQualification: true,
		Fields: []DeltaField{{"id", "long"}, {"region", "string"}, {"email", "string"}, {"amount", "double"}, {"active", "boolean"}, {"ordered_on", "date"}}}
	observation, err := client.InspectDelta(context.Background(), target, "dataslate-publisher", "dataslate-live-publication")
	if err != nil {
		t.Fatal(err)
	}
	if !observation.Exists || !observation.VersionPresent || len(observation.Fields) != 6 {
		t.Fatalf("incomplete readback: %+v", observation)
	}
	t.Log("Verified synthetic Delta location, retained version, ordered schema, protocol and feature union through native TLS adapter")
	target.Location = "file:///lake/wrong-location/"
	if _, err := client.InspectDelta(context.Background(), target, "dataslate-publisher", "dataslate-live-mismatch"); err != ErrDeltaMetadata {
		t.Fatalf("wrong location did not fail closed: %v", err)
	}
}
