package trino

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"path"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var ErrDeltaTarget = errors.New("invalid or unsupported Delta publication target")
var ErrDeltaMetadata = errors.New("Delta catalog metadata does not match the verified ingestion outcome")

var deltaIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
var deltaVersion = regexp.MustCompile(`^(0|[1-9][0-9]{0,18})$`)
var deltaFeature = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,63}$`)
var deltaPathSegment = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9_.=-]*$`)
var deltaBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

type DeltaField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// PublishTarget is constructed from administrator configuration and an owned
// runtime outcome, never from caller-supplied SQL, locations or engine identity.
type PublishTarget struct {
	Catalog, Schema, Table, Location, Version string
	Fields                                    []DeltaField
	MinReaderVersion, MinWriterVersion        int
	ReaderFeatures, WriterFeatures            []string
	// Only an explicitly invoked local transport qualification may set this.
	// It has no JSON representation and is never a production binding option.
	AllowLocalQualification bool `json:"-"`
}

// DeltaObservation verifies catalog location, a retained version, schema and
// protocol. Trino exposes the union of table features, not their reader/writer
// categories, and does not expose the native Delta UUID through these queries.
type DeltaObservation struct {
	Exists, VersionPresent             bool
	Location                           string
	Fields                             []DeltaField
	MinReaderVersion, MinWriterVersion int
	Features                           []string
}

func deltaLocation(raw string, allowLocal bool) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || u.Opaque != "" || u.Path == "" || !strings.HasPrefix(u.Path, "/") {
		return "", ErrDeltaTarget
	}
	if u.Scheme == "file" && allowLocal {
		if u.Host != "" || raw != "file://"+u.Path {
			return "", ErrDeltaTarget
		}
	} else if u.Scheme != "s3" || !deltaBucket.MatchString(u.Host) || net.ParseIP(u.Host) != nil || strings.Contains(u.Host, "..") || strings.Contains(u.Host, ".-") || strings.Contains(u.Host, "-.") || raw != "s3://"+u.Host+u.Path {
		return "", ErrDeltaTarget
	}
	trimmed := strings.TrimSuffix(u.Path, "/")
	if trimmed == "" || path.Clean(trimmed) != trimmed {
		return "", ErrDeltaTarget
	}
	for _, part := range strings.Split(strings.TrimPrefix(trimmed, "/"), "/") {
		if !deltaPathSegment.MatchString(part) {
			return "", ErrDeltaTarget
		}
	}
	u.Path = trimmed
	return u.String(), nil
}

func (t PublishTarget) Validate() error {
	for _, name := range []string{t.Catalog, t.Schema, t.Table} {
		if !deltaIdentifier.MatchString(name) {
			return ErrDeltaTarget
		}
	}
	if _, err := deltaLocation(t.Location, t.AllowLocalQualification); err != nil {
		return err
	}
	if !deltaVersion.MatchString(t.Version) {
		return ErrDeltaTarget
	}
	if _, err := strconv.ParseInt(t.Version, 10, 64); err != nil {
		return ErrDeltaTarget
	}
	if len(t.Fields) == 0 || len(t.Fields) > 200 || t.MinReaderVersion < 1 || t.MinReaderVersion > 3 || t.MinWriterVersion < 1 || t.MinWriterVersion > 7 {
		return ErrDeltaTarget
	}
	if (len(t.ReaderFeatures) > 0 && t.MinReaderVersion != 3) || (len(t.WriterFeatures) > 0 && t.MinWriterVersion != 7) || (t.MinReaderVersion == 3 && t.MinWriterVersion != 7) {
		return ErrDeltaTarget
	}
	seen := map[string]bool{}
	for _, field := range t.Fields {
		key := strings.ToLower(field.Name)
		if !deltaIdentifier.MatchString(field.Name) || seen[key] {
			return ErrDeltaTarget
		}
		seen[key] = true
		switch field.Type {
		case "string", "long", "double", "boolean", "date":
		default:
			// Trino 484 maps Delta timestamp to millisecond precision. Do not
			// silently certify a microsecond-preserving Spark table as equivalent.
			return ErrDeltaTarget
		}
	}
	for _, features := range [][]string{t.ReaderFeatures, t.WriterFeatures} {
		if len(features) > 32 {
			return ErrDeltaTarget
		}
		seen := map[string]bool{}
		for _, feature := range features {
			if !deltaFeature.MatchString(feature) || seen[feature] {
				return ErrDeltaTarget
			}
			seen[feature] = true
		}
	}
	return nil
}

func quotedDeltaTable(t PublishTarget, suffix string) string {
	return `"` + t.Catalog + `"."` + t.Schema + `"."` + t.Table + suffix + `"`
}

// Register sends exactly one mutation POST. Persist dispatch intent first and
// never repeat this call automatically when its result is unknown.
func (c *Client) Register(ctx context.Context, target PublishTarget, user, trace string) (Page, error) {
	if err := target.Validate(); err != nil {
		return Page{}, err
	}
	location, _ := deltaLocation(target.Location, target.AllowLocalQualification)
	statement := `CALL "` + target.Catalog + `".system.register_table(schema_name => '` + target.Schema + `', table_name => '` + target.Table + `', table_location => '` + location + `')`
	return c.Submit(ctx, Statement{SQL: statement, User: user, Catalog: target.Catalog, Schema: target.Schema, TraceToken: trace, MaxRunSeconds: 60})
}

type deltaQueryError struct{ name string }

func (e deltaQueryError) Error() string { return "Delta metadata query was rejected by the engine" }

// deltaRead runs only internally generated read-only metadata SQL. It is bounded
// independently of the general result API and never persists engine row values.
func (c *Client) deltaRead(ctx context.Context, target PublishTarget, user, trace, sql string, maxRows int) (Page, error) {
	page, err := c.Submit(ctx, Statement{SQL: sql, User: user, Catalog: target.Catalog, Schema: target.Schema, TraceToken: trace, MaxRunSeconds: 60})
	if err != nil {
		return Page{}, err
	}
	cursor := page.Cursor
	finished := false
	defer func() {
		if !finished && cursor.NextURI != "" {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = c.Cancel(cleanup, cursor, user)
		}
	}()
	out := Page{Cursor: Cursor{ID: page.ID}, Rows: [][]json.RawMessage{}}
	seen := map[string]bool{}
	bytes := 0
	for pages := 0; pages < 32; pages++ {
		cursor = page.Cursor
		if page.Error != nil {
			if page.NextURI != "" || len(page.Rows) != 0 {
				return Page{}, ErrDeltaMetadata
			}
			finished = true
			return Page{}, deltaQueryError{name: page.Error.Name}
		}
		if len(page.Columns) > 0 {
			if len(out.Columns) > 0 && !reflect.DeepEqual(out.Columns, page.Columns) {
				return Page{}, ErrDeltaMetadata
			}
			out.Columns = page.Columns
		}
		for _, row := range page.Rows {
			encoded, _ := json.Marshal(row)
			bytes += len(encoded)
			if len(out.Rows) >= maxRows || bytes > 128*1024 || len(row) != len(out.Columns) {
				return Page{}, ErrDeltaMetadata
			}
			out.Rows = append(out.Rows, row)
		}
		if page.NextURI == "" {
			finished = true
			return out, nil
		}
		if seen[page.NextURI] {
			return Page{}, ErrDeltaMetadata
		}
		seen[page.NextURI] = true
		cursor = page.Cursor
		page, err = c.Next(ctx, cursor, user)
		if err != nil {
			return Page{}, err
		}
	}
	return Page{}, ErrDeltaMetadata
}

func deltaString(raw json.RawMessage) (string, error) {
	var value string
	if len(raw) > 8192 || json.Unmarshal(raw, &value) != nil || string(raw) == "null" || strings.ContainsRune(value, 0) {
		return "", ErrDeltaMetadata
	}
	return value, nil
}

func deltaColumns(page Page, kind string, names ...string) bool {
	if len(page.Columns) != len(names) {
		return false
	}
	for i, name := range names {
		if page.Columns[i].Name != name || page.Columns[i].Type != kind {
			return false
		}
	}
	return true
}

// InspectDelta verifies the current catalog mapping without registering or
// modifying anything. Missing source history or changed metadata is a mismatch.
// Exists=false is reserved for an explicit engine not-found result on the first
// fully-qualified target query; access and transport failures are never absence.
func (c *Client) InspectDelta(ctx context.Context, target PublishTarget, user, trace string) (DeltaObservation, error) {
	if err := target.Validate(); err != nil {
		return DeltaObservation{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 18*time.Second)
	defer cancel()
	properties, err := c.deltaRead(ctx, target, user, trace+"-p", "SELECT key, value FROM "+quotedDeltaTable(target, "$properties"), 1024)
	if err != nil {
		var rejected deltaQueryError
		if errors.As(err, &rejected) && (rejected.name == "TABLE_NOT_FOUND" || rejected.name == "SCHEMA_NOT_FOUND") {
			return DeltaObservation{Exists: false}, nil
		}
		return DeltaObservation{}, err
	}
	if !deltaColumns(properties, "varchar", "key", "value") {
		return DeltaObservation{}, ErrDeltaMetadata
	}
	values := map[string]string{}
	for _, row := range properties.Rows {
		key, e1 := deltaString(row[0])
		value, e2 := deltaString(row[1])
		if _, exists := values[key]; exists || key == "" || e1 != nil || e2 != nil {
			return DeltaObservation{}, ErrDeltaMetadata
		}
		values[key] = value
	}
	location, err := deltaLocation(values["location"], target.AllowLocalQualification)
	expectedLocation, _ := deltaLocation(target.Location, target.AllowLocalQualification)
	if err != nil || location != expectedLocation {
		return DeltaObservation{}, ErrDeltaMetadata
	}
	reader, e1 := strconv.Atoi(values["delta.minReaderVersion"])
	writer, e2 := strconv.Atoi(values["delta.minWriterVersion"])
	if e1 != nil || e2 != nil || strconv.Itoa(reader) != values["delta.minReaderVersion"] || strconv.Itoa(writer) != values["delta.minWriterVersion"] || reader != target.MinReaderVersion || writer != target.MinWriterVersion {
		return DeltaObservation{}, ErrDeltaMetadata
	}
	features := []string{}
	for key, value := range values {
		if strings.HasPrefix(key, "delta.feature.") {
			feature := strings.TrimPrefix(key, "delta.feature.")
			if !deltaFeature.MatchString(feature) || value != "supported" {
				return DeltaObservation{}, ErrDeltaMetadata
			}
			features = append(features, feature)
		}
	}
	expectedFeatures := map[string]bool{}
	for _, feature := range append(append([]string{}, target.ReaderFeatures...), target.WriterFeatures...) {
		expectedFeatures[feature] = true
	}
	if len(features) != len(expectedFeatures) {
		return DeltaObservation{}, ErrDeltaMetadata
	}
	for _, feature := range features {
		if !expectedFeatures[feature] {
			return DeltaObservation{}, ErrDeltaMetadata
		}
	}
	sort.Strings(features)
	history, err := c.deltaRead(ctx, target, user, trace+"-h", "SELECT version FROM "+quotedDeltaTable(target, "$history")+" WHERE version = "+target.Version+" LIMIT 2", 2)
	if err != nil {
		return DeltaObservation{}, err
	}
	if !deltaColumns(history, "bigint", "version") || len(history.Rows) != 1 || string(history.Rows[0][0]) != target.Version {
		return DeltaObservation{}, ErrDeltaMetadata
	}
	schema, err := c.deltaRead(ctx, target, user, trace+"-s", "DESCRIBE "+quotedDeltaTable(target, ""), 200)
	if err != nil {
		return DeltaObservation{}, err
	}
	if !deltaColumns(schema, "varchar", "Column", "Type", "Extra", "Comment") || len(schema.Rows) != len(target.Fields) {
		return DeltaObservation{}, ErrDeltaMetadata
	}
	types := map[string]string{"varchar": "string", "bigint": "long", "double": "double", "boolean": "boolean", "date": "date"}
	fields := make([]DeltaField, 0, len(schema.Rows))
	for i, row := range schema.Rows {
		name, e1 := deltaString(row[0])
		typeName, e2 := deltaString(row[1])
		extra, e3 := deltaString(row[2])
		if e1 != nil || e2 != nil || e3 != nil || extra != "" || name != target.Fields[i].Name || types[typeName] != target.Fields[i].Type {
			return DeltaObservation{}, ErrDeltaMetadata
		}
		fields = append(fields, DeltaField{Name: name, Type: types[typeName]})
	}
	return DeltaObservation{Exists: true, VersionPresent: true, Location: location, Fields: fields, MinReaderVersion: reader, MinWriterVersion: writer, Features: features}, nil
}
