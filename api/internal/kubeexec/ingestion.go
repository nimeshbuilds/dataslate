package kubeexec

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"dataslate.local/api/internal/platform"
)

// DrainIngestion is the sole release gate for the control-plane ingestion writer.
// A terminal Spark status alone does not prove driver/executor process absence.
// Never adopt a replacement application UID or infer absence after ambiguous POST.
func (c *Client) DrainIngestion(ctx context.Context, r platform.Run) (bool, error) {
	if !r.Terminal() || r.Ingestion == nil || r.Profile.Kind() != platform.FileIngestionBatch {
		return false, errors.New("ingestion drain requires a terminal ingestion run")
	}
	if r.SubmissionStarted && r.KubernetesUID == "" {
		return false, errors.New("ambiguous submission retains ingestion writer")
	}
	app, exists, err := c.get(ctx, r)
	if err != nil {
		return false, err
	}
	if exists {
		if r.KubernetesUID == "" || app.Metadata.UID != r.KubernetesUID {
			return false, errors.New("ingestion application identity is not durably known")
		}
		if app.Metadata.DeletionTimestamp == "" {
			input := map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground", "preconditions": map[string]string{"uid": r.KubernetesUID}}
			if err = c.request(ctx, http.MethodDelete, appPath(r)+"/"+r.KubernetesName, input, nil); err != nil && !statusIs(err, 404) {
				return false, err
			}
		}
		if _, exists, err = c.get(ctx, r); err != nil || exists {
			return false, err
		}
	}
	selector := url.Values{"labelSelector": []string{"dataslate.io/run=" + r.ID}, "limit": []string{"1000"}}
	var pods struct {
		Items    []pod `json:"items"`
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
	}
	if err = c.request(ctx, http.MethodGet, "/api/v1/namespaces/"+r.Profile.Namespace+"/pods?"+selector.Encode(), nil, &pods); err != nil {
		return false, err
	}
	// Even an unexpected/replaced pod retains the writer: absence is required and
	// this endpoint never adopts or deletes pods based on their labels alone.
	if len(pods.Items) > 0 || pods.Metadata.Continue != "" {
		return false, nil
	}
	// Recheck the deterministic application identity after observing pod absence.
	if _, exists, err = c.get(ctx, r); err != nil || exists {
		return false, err
	}
	return true, nil
}
