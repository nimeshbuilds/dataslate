package kubeexec

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"time"

	"dataslate.local/api/internal/platform"
)

var _ platform.SourceBackend = (*Client)(nil)

type sourceConfigMap struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   objectMeta        `json:"metadata"`
	Immutable  bool              `json:"immutable"`
	Data       map[string]string `json:"data"`
	BinaryData map[string]string `json:"binaryData,omitempty"`
}

func sourcePath(r platform.Run) string {
	return "/api/v1/namespaces/" + r.Profile.Namespace + "/configmaps"
}
func (c *Client) sourceManifest(r platform.Run) (sourceConfigMap, error) {
	if err := r.ValidateSource(); err != nil {
		return sourceConfigMap{}, err
	}
	app, err := c.manifest(r)
	if err != nil {
		return sourceConfigMap{}, err
	}
	return sourceConfigMap{APIVersion: "v1", Kind: "ConfigMap", Metadata: objectMeta{Name: r.SourceArtifact.Name, Namespace: r.Profile.Namespace, Labels: app.Metadata.Labels, Annotations: map[string]string{"dataslate.io/source-sha256": r.SourceSHA256()}}, Immutable: true, Data: map[string]string{"source.py": r.Source.Source}}, nil
}
func sourceOwner(r platform.Run) ownerReference {
	return ownerReference{APIVersion: "spark.apache.org/v1", Kind: "SparkApplication", Name: r.KubernetesName, UID: r.KubernetesUID, Controller: true, BlockOwnerDeletion: false}
}
func verifySource(actual, expected sourceConfigMap, r platform.Run) error {
	m := actual.Metadata
	if actual.APIVersion != "v1" || actual.Kind != "ConfigMap" || m.Name != expected.Metadata.Name || m.Namespace != expected.Metadata.Namespace || m.UID == "" || len(m.UID) > 256 || m.ResourceVersion == "" || m.DeletionTimestamp != "" || r.SourceArtifact.UID != "" && m.UID != r.SourceArtifact.UID {
		return errors.New("execution source identity mismatch")
	}
	if !actual.Immutable || !reflect.DeepEqual(actual.Data, expected.Data) || len(actual.BinaryData) > 0 || m.Annotations["dataslate.io/source-sha256"] != r.SourceSHA256() {
		return errors.New("immutable execution source changed")
	}
	for k, v := range expected.Metadata.Labels {
		if m.Labels[k] != v {
			return errors.New("execution source ownership mismatch")
		}
	}
	if len(m.OwnerReferences) > 0 && (r.KubernetesUID == "" || len(m.OwnerReferences) != 1 || m.OwnerReferences[0] != sourceOwner(r)) {
		return errors.New("execution source owner reference mismatch")
	}
	return nil
}
func (c *Client) getSource(ctx context.Context, r platform.Run) (sourceConfigMap, bool, error) {
	expected, err := c.sourceManifest(r)
	if err != nil {
		return sourceConfigMap{}, false, err
	}
	var actual sourceConfigMap
	err = c.request(ctx, http.MethodGet, sourcePath(r)+"/"+r.SourceArtifact.Name, nil, &actual)
	if statusIs(err, 404) {
		return sourceConfigMap{}, false, nil
	}
	if err != nil {
		return sourceConfigMap{}, false, err
	}
	if err = verifySource(actual, expected, r); err != nil {
		return sourceConfigMap{}, false, err
	}
	return actual, true, nil
}
func (c *Client) ObserveSource(ctx context.Context, r platform.Run) (platform.SourceObservation, error) {
	a, exists, err := c.getSource(ctx, r)
	return platform.SourceObservation{Exists: exists, UID: a.Metadata.UID}, err
}
func (c *Client) PrepareSource(ctx context.Context, r platform.Run) (platform.SourceArtifact, error) {
	return c.prepareSource(ctx, r, nil)
}
func (c *Client) prepareSource(ctx context.Context, r platform.Run, authorize func(context.Context, platform.Run) (bool, error)) (platform.SourceArtifact, error) {
	if r.CancelRequested || r.Terminal() || r.SubmissionStarted || !time.Now().Before(r.ExecutionDeadline()) || r.SourceArtifact == nil || !r.SourceArtifact.CreationStarted {
		return platform.SourceArtifact{}, errors.New("run does not permit source preparation")
	}
	expected, err := c.sourceManifest(r)
	if err != nil {
		return platform.SourceArtifact{}, err
	}
	actual, exists, err := c.getSource(ctx, r)
	if err != nil {
		return platform.SourceArtifact{}, err
	}
	ready := func(a sourceConfigMap) platform.SourceArtifact {
		return platform.SourceArtifact{Name: a.Metadata.Name, UID: a.Metadata.UID, State: "ready", CreationStarted: true}
	}
	if exists {
		return ready(actual), nil
	}
	if r.SourceArtifact.UID != "" {
		return platform.SourceArtifact{}, errors.New("previously prepared source is absent; replacement refused")
	}
	if authorize != nil {
		allowed, e := authorize(ctx, r)
		if e != nil {
			return platform.SourceArtifact{}, e
		}
		if !allowed {
			return platform.SourceArtifact{}, errors.New("source preparation authorization revoked")
		}
	}
	if !time.Now().Before(r.ExecutionDeadline()) {
		return platform.SourceArtifact{}, errors.New("run lifetime elapsed before source creation")
	}
	err = c.request(ctx, http.MethodPost, sourcePath(r), expected, &actual)
	if err == nil {
		if err = verifySource(actual, expected, r); err != nil {
			return platform.SourceArtifact{}, err
		}
		return ready(actual), nil
	}
	// Source creation has no execution side effects. An exact immutable object may be
	// adopted after an uncertain response. No second POST is made during this call.
	actual, exists, getErr := c.getSource(ctx, r)
	if getErr == nil && exists {
		return ready(actual), nil
	}
	return platform.SourceArtifact{}, errors.New("source preparation outcome is unconfirmed")
}
func (c *Client) BindSource(ctx context.Context, r platform.Run) error {
	if r.KubernetesUID == "" || r.SourceArtifact == nil || r.SourceArtifact.UID == "" {
		return errors.New("source binding requires durable application and source UIDs")
	}
	if _, exists, err := c.get(ctx, r); err != nil || !exists {
		return errors.New("application identity unavailable for source binding")
	}
	actual, exists, err := c.getSource(ctx, r)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("prepared source is absent")
	}
	if len(actual.Metadata.OwnerReferences) == 1 {
		return nil
	}
	patch := []any{
		map[string]any{"op": "test", "path": "/metadata/uid", "value": actual.Metadata.UID},
		map[string]any{"op": "test", "path": "/metadata/resourceVersion", "value": actual.Metadata.ResourceVersion},
		map[string]any{"op": "add", "path": "/metadata/ownerReferences", "value": []ownerReference{sourceOwner(r)}},
	}
	var updated sourceConfigMap
	err = c.request(ctx, http.MethodPatch, sourcePath(r)+"/"+actual.Metadata.Name, patch, &updated)
	// A response failure may still have patched. Re-read using the immutable source UID.
	if err != nil {
		updated, exists, err = c.getSource(ctx, r)
		if err != nil || !exists {
			return errors.New("source owner binding is unconfirmed")
		}
	}
	expected, _ := c.sourceManifest(r)
	if err = verifySource(updated, expected, r); err != nil {
		return err
	}
	if len(updated.Metadata.OwnerReferences) != 1 {
		return errors.New("source owner binding is unconfirmed")
	}
	return nil
}
func (c *Client) DeleteSource(ctx context.Context, r platform.Run) error {
	if !r.Terminal() || r.SourceArtifact == nil || r.SourceArtifact.UID == "" {
		return errors.New("source cleanup requires terminal evidence and a durable source UID")
	}
	actual, exists, err := c.getSource(ctx, r)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	input := map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]string{"uid": actual.Metadata.UID, "resourceVersion": actual.Metadata.ResourceVersion}}
	err = c.request(ctx, http.MethodDelete, sourcePath(r)+"/"+actual.Metadata.Name, input, nil)
	if err != nil && !statusIs(err, 404) {
		return err
	}
	_, exists, err = c.getSource(ctx, r)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("source deletion remains pending")
	}
	return nil
}
