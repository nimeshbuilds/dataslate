package kubeexec

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"dataslate.local/api/internal/platform"
)

// CaptureIngestionOutcome reads the complete fixed termination file from an
// owned successful driver. Ordinary log tails are never receipt evidence.
func (c *Client) CaptureIngestionOutcome(ctx context.Context, r platform.Run) (platform.IngestionTermination, error) {
	if r.Ingestion == nil || r.State != "succeeded" || r.KubernetesUID == "" || r.ValidateSource() != nil {
		return platform.IngestionTermination{}, platform.ErrIngestionOutcome
	}
	a, exists, e := c.get(ctx, r)
	if e != nil {
		return platform.IngestionTermination{}, e
	}
	if !exists {
		return platform.IngestionTermination{Final: true}, nil
	}
	if observe(a).State != "succeeded" {
		return platform.IngestionTermination{}, errors.New("operator success is not currently confirmed")
	}
	selector := url.Values{"labelSelector": []string{"spark.operator/spark-app-name=" + r.KubernetesName + ",spark-role=driver"}, "limit": []string{"20"}}
	var list struct {
		Items    []pod `json:"items"`
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
	}
	prefix := "/api/v1/namespaces/" + r.Profile.Namespace + "/pods"
	if e = c.request(ctx, http.MethodGet, prefix+"?"+selector.Encode(), nil, &list); e != nil {
		return platform.IngestionTermination{}, e
	}
	if list.Metadata.Continue != "" || len(list.Items) > 20 {
		return platform.IngestionTermination{}, errors.New("ambiguous driver evidence")
	}
	var selected *pod
	for i := range list.Items {
		p := &list.Items[i]
		if ownedDriver(*p, r, a.Metadata.UID) {
			if selected != nil {
				return platform.IngestionTermination{}, errors.New("multiple owned driver evidence")
			}
			selected = p
		}
	}
	if selected == nil {
		return platform.IngestionTermination{}, errors.New("driver evidence is not available")
	}
	var current pod
	if e = c.request(ctx, http.MethodGet, prefix+"/"+selected.Metadata.Name, nil, &current); e != nil {
		return platform.IngestionTermination{}, e
	}
	if !ownedDriver(current, r, r.KubernetesUID) || current.Metadata.UID != selected.Metadata.UID || current.Metadata.Labels["dataslate.io/run"] != r.ID {
		return platform.IngestionTermination{}, errors.New("driver evidence identity changed")
	}
	driverContainers := 0
	for _, container := range current.Spec.Containers {
		if container.Name == "spark-kubernetes-driver" {
			driverContainers++
			if container.Image != r.Profile.Image || container.TerminationMessagePath != "/dev/termination-log" || container.TerminationMessagePolicy != "File" {
				return platform.IngestionTermination{}, errors.New("driver image or termination file policy differs from the approved runtime")
			}
		}
	}
	if driverContainers != 1 {
		return platform.IngestionTermination{}, errors.New("driver container identity is ambiguous")
	}
	t := platform.IngestionTermination{ApplicationUID: r.KubernetesUID, DriverUID: current.Metadata.UID, DriverName: current.Metadata.Name}
	found := false
	for _, status := range current.Status.ContainerStatuses {
		if status.Name == "spark-kubernetes-driver" {
			if found || status.RestartCount != 0 {
				return platform.IngestionTermination{}, errors.New("driver evidence restarted or ambiguous")
			}
			found = true
			if status.State.Terminated != nil {
				t.Final = true
				t.ExitCode = status.State.Terminated.ExitCode
				t.Message = status.State.Terminated.Message
			}
		}
	}
	// Verify immutable application identity again after reading the pod status.
	after, present, e := c.get(ctx, r)
	if e != nil || !present || after.Metadata.UID != a.Metadata.UID || observe(after).State != "succeeded" {
		return platform.IngestionTermination{}, errors.New("application evidence identity changed")
	}
	if len(t.Message) > platform.MaxIngestionReceiptBytes {
		return platform.IngestionTermination{}, errors.New("driver receipt exceeds bound")
	}
	return t, nil
}
