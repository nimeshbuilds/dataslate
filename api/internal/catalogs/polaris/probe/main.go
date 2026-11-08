// Command probe performs bounded read-only checks through the production client.
package main

import (
	"context"
	"dataslate.local/api/internal/catalogs/polaris"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	configPath := flag.String("config", "", "mounted/file-based catalog config")
	namespace := flag.String("namespace", "[]", "JSON namespace components")
	table := flag.String("table", "", "existing native Iceberg table")
	flag.Parse()
	if *configPath == "" || *table == "" {
		fail("config and table are required")
	}
	f, e := os.Open(*configPath)
	if e != nil {
		fail("config unavailable")
	}
	defer f.Close()
	var cfg polaris.Config
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if d.Decode(&cfg) != nil {
		fail("invalid config")
	}
	var ns []string
	if json.Unmarshal([]byte(*namespace), &ns) != nil {
		fail("invalid namespace")
	}
	client, e := polaris.New(cfg)
	if e != nil {
		fail(e.Error())
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	caps, e := client.Discover(ctx)
	if e != nil {
		fail(e.Error())
	}
	roots, e := client.ListNamespaces(ctx, nil, "", 100)
	if e != nil {
		fail(e.Error())
	}
	tables, e := client.ListTables(ctx, ns, "", 100)
	if e != nil {
		fail(e.Error())
	}
	metadata, e := client.LoadTable(ctx, polaris.Identifier{Namespace: ns, Name: *table})
	if e != nil {
		fail(e.Error())
	}
	result := map[string]any{"discovered": true, "catalog": caps.Catalog, "rootNamespaces": len(roots.Namespaces), "tablesInRequestedNamespace": len(tables.Identifiers), "tableFormatVersion": metadata.FormatVersion, "tableUUID": metadata.UUID, "schemaFields": len(metadata.CurrentSchema.Fields), "credentialVending": false, "readOnly": true}
	json.NewEncoder(os.Stdout).Encode(result)
}
func fail(message string) { fmt.Fprintln(os.Stderr, message); os.Exit(1) }
