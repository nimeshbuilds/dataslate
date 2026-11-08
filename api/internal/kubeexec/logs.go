package kubeexec

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	"dataslate.local/api/internal/platform"
)

const logLimit = 64 * 1024

type pod struct {
	Metadata objectMeta `json:"metadata"`
	Spec     struct {
		Containers []struct {
			Name string `json:"name"`
		} `json:"containers"`
	} `json:"spec"`
}

func ownedDriver(p pod, r platform.Run, applicationUID string) bool {
	if !dnsName.MatchString(p.Metadata.Name) || p.Metadata.Namespace != r.Profile.Namespace || p.Metadata.UID == "" || p.Metadata.Labels["spark-role"] != "driver" || p.Metadata.Labels["spark.operator/spark-app-name"] != r.KubernetesName {
		return false
	}
	owned := false
	for _, o := range p.Metadata.OwnerReferences {
		if o.APIVersion == "spark.apache.org/v1" && o.Kind == "SparkApplication" && o.Name == r.KubernetesName && o.UID == applicationUID {
			owned = true
		}
	}
	container := false
	for _, c := range p.Spec.Containers {
		if c.Name == "spark-kubernetes-driver" {
			container = true
		}
	}
	return owned && container
}

// Logs returns a bounded tail from the actual driver's standard container. A
// label alone never authorizes log access: both application and pod ownership
// are verified. Logs are intentionally unavailable after upstream cleanup.
func (c *Client) Logs(ctx context.Context, r platform.Run) (platform.RunLogs, error) {
	a, exists, err := c.get(ctx, r)
	if err != nil {
		return platform.RunLogs{}, err
	}
	if !exists {
		return platform.RunLogs{}, platform.ErrCapabilityUnavailable
	}
	if r.KubernetesUID == "" {
		return platform.RunLogs{}, platform.ErrCapabilityUnavailable
	}
	selector := url.Values{"labelSelector": []string{"spark.operator/spark-app-name=" + r.KubernetesName + ",spark-role=driver"}, "limit": []string{"20"}}
	var list struct {
		Items    []pod `json:"items"`
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
	}
	prefix := "/api/v1/namespaces/" + r.Profile.Namespace + "/pods"
	if err = c.request(ctx, http.MethodGet, prefix+"?"+selector.Encode(), nil, &list); err != nil {
		return platform.RunLogs{}, err
	}
	if list.Metadata.Continue != "" {
		return platform.RunLogs{}, errors.New("driver pod selection is ambiguous")
	}
	var driver *pod
	for i := range list.Items {
		if ownedDriver(list.Items[i], r, a.Metadata.UID) {
			if driver != nil {
				return platform.RunLogs{}, errors.New("multiple owned driver pods; log selection is ambiguous")
			}
			driver = &list.Items[i]
		}
	}
	if driver == nil {
		return platform.RunLogs{}, platform.ErrCapabilityUnavailable
	}
	// Recheck immediately before and after the log request to discard data if the
	// pod name was replaced. Kubernetes PodLogOptions has no UID precondition.
	var before pod
	podPath := prefix + "/" + driver.Metadata.Name
	if err = c.request(ctx, http.MethodGet, podPath, nil, &before); err != nil {
		return platform.RunLogs{}, err
	}
	if before.Metadata.UID != driver.Metadata.UID || !ownedDriver(before, r, a.Metadata.UID) {
		return platform.RunLogs{}, errors.New("driver identity changed")
	}
	query := url.Values{"container": []string{"spark-kubernetes-driver"}, "tailLines": []string{"500"}, "limitBytes": []string{"65536"}, "timestamps": []string{"true"}}
	b, err := c.requestBytes(ctx, http.MethodGet, podPath+"/log?"+query.Encode(), nil, logLimit)
	if err != nil {
		return platform.RunLogs{}, err
	}
	var after pod
	if err = c.request(ctx, http.MethodGet, podPath, nil, &after); err != nil {
		return platform.RunLogs{}, err
	}
	if after.Metadata.UID != before.Metadata.UID || !ownedDriver(after, r, a.Metadata.UID) {
		return platform.RunLogs{}, errors.New("driver identity changed while reading logs")
	}
	text := string(b)
	if !utf8.Valid(b) {
		text = strings.ToValidUTF8(text, "�")
	}
	// The endpoint always requests a tail, so disclose that this is a partial log
	// view even if the API returned fewer bytes than its maximum.
	return platform.RunLogs{Text: text, Truncated: true}, nil
}
