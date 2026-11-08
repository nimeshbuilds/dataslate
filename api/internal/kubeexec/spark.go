package kubeexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"dataslate.local/api/internal/platform"
)

var _ platform.ExecutionBackend = (*Client)(nil)
var runID = regexp.MustCompile(`^run-[a-f0-9]{32}$`)

// ValidateBindings fails startup if the serving API and cluster access namespace
// allowlists disagree; accepting an unusable job is not successful configuration.
func (c *Client) ValidateBindings(bindings []platform.ExecutionBinding) error {
	for _, b := range bindings {
		if err := b.Validate(); err != nil {
			return errors.New("execution binding is invalid")
		}
		// Empty grants are an intentional deny-all policy. This synthetic identity
		// is used only to validate profile compilation, never to authorize or submit.
		subject := platform.ExecutionSubject{Issuer: "https://validation.invalid", Subject: "binding-validation"}
		if len(b.AllowedSubjects) > 0 {
			subject = b.AllowedSubjects[0]
		}
		r := platform.Run{Resource: platform.Resource{ID: "run-00000000000000000000000000000000"}, WorkspaceID: b.WorkspaceID, BindingID: b.ID, SubmittedBy: subject, Profile: b.ExecutionProfile, KubernetesName: "ds-00000000000000000000000000000000"}
		if b.Kind() == platform.NotebookPythonBatch {
			r.Notebook = &platform.NotebookProvenance{SHA256: strings.Repeat("0", 64)}
			r.SourceArtifact = &platform.SourceArtifact{Name: r.KubernetesName + "-source", State: "pending"}
		}
		if _, err := c.manifest(r); err != nil {
			return errors.New("execution binding does not match Kubernetes client policy")
		}
	}
	return nil
}

type ownerReference struct {
	APIVersion         string `json:"apiVersion"`
	Kind               string `json:"kind"`
	Name               string `json:"name"`
	UID                string `json:"uid"`
	Controller         bool   `json:"controller"`
	BlockOwnerDeletion bool   `json:"blockOwnerDeletion"`
}
type objectMeta struct {
	ResourceVersion   string            `json:"resourceVersion,omitempty"`
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	UID               string            `json:"uid,omitempty"`
	Labels            map[string]string `json:"labels"`
	Annotations       map[string]string `json:"annotations"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
	OwnerReferences   []ownerReference  `json:"ownerReferences,omitempty"`
}
type appState struct {
	Summary string `json:"currentStateSummary"`
}
type application struct {
	APIVersion string         `json:"apiVersion"`
	Kind       string         `json:"kind"`
	Metadata   objectMeta     `json:"metadata"`
	Spec       map[string]any `json:"spec"`
	Status     struct {
		Current appState            `json:"currentState"`
		History map[string]appState `json:"stateTransitionHistory"`
	} `json:"status,omitzero"`
}

// manifest accepts only an immutable administrator-approved Python image profile.
// No user Spark configuration, arguments, pod template or credential is forwarded.
func (c *Client) manifest(r platform.Run) (application, error) {
	if !runID.MatchString(r.ID) || r.KubernetesName != "ds-"+strings.TrimPrefix(r.ID, "run-") || !dnsName.MatchString(r.WorkspaceID) || !dnsName.MatchString(r.BindingID) || !c.namespaces[r.Profile.Namespace] {
		return application{}, errors.New("run identity or execution namespace is not approved")
	}
	binding := platform.ExecutionBinding{ID: r.BindingID, WorkspaceID: r.WorkspaceID, Name: "immutable run profile", ExecutionProfile: r.Profile, AllowedSubjects: []platform.ExecutionSubject{r.SubmittedBy}}
	if err := binding.Validate(); err != nil {
		return application{}, errors.New("run profile is invalid")
	}
	mainFile := r.Profile.ApplicationFile()
	u, err := url.Parse(mainFile)
	if err != nil || u.RawPath != "" || mainFile != "local://"+u.Path {
		return application{}, errors.New("application URI must be canonical and local to the image")
	}
	p, err := platform.CompileSpark(platform.SparkInput{Name: r.KubernetesName, Operator: r.Profile.Operator, Image: r.Profile.Image, MainApplicationFile: mainFile, ServiceAccount: r.Profile.ServiceAccount, Namespace: r.Profile.Namespace, ExecutorInstances: r.Profile.ExecutorInstances}, r.WorkspaceID)
	if err != nil {
		return application{}, errors.New("run profile cannot be compiled")
	}
	spec := p.Manifest["spec"].(map[string]any)
	conf := spec["sparkConf"].(map[string]string)
	conf["spark.driver.port"] = "7078"
	conf["spark.blockManager.port"] = "7079"
	conf["spark.kubernetes.authenticate.executor.serviceAccountName"] = r.Profile.ServiceAccount
	conf["spark.kubernetes.driver.podTemplateContainerName"] = "spark-kubernetes-driver"
	conf["spark.kubernetes.executor.podTemplateContainerName"] = "executor"
	spec["driverSpec"] = restrictedTemplate("spark-kubernetes-driver", true)
	if r.Profile.Kind() == platform.NotebookPythonBatch {
		if r.Notebook == nil || r.SourceArtifact == nil || r.SourceArtifact.Name != r.KubernetesName+"-source" || len(r.Notebook.SHA256) != 64 {
			return application{}, errors.New("notebook source provenance is missing")
		}
		spec["mainClass"] = "org.apache.spark.deploy.PythonRunner"
		driver := spec["driverSpec"].(map[string]any)["podTemplateSpec"].(map[string]any)["spec"].(map[string]any)
		driver["volumes"] = []any{map[string]any{"name": "dataslate-source", "configMap": map[string]any{"name": r.SourceArtifact.Name, "items": []any{map[string]string{"key": "source.py", "path": "source.py"}}}}}
		driver["containers"].([]any)[0].(map[string]any)["volumeMounts"] = []any{map[string]any{"name": "dataslate-source", "mountPath": "/opt/dataslate/source", "readOnly": true}}
	}
	spec["executorSpec"] = restrictedTemplate("executor", false)
	spec["deploymentMode"] = "ClusterMode"
	spec["applicationTolerations"] = map[string]any{"resourceRetainPolicy": "OnFailure", "resourceRetainDurationMillis": 3600000, "ttlAfterStopMillis": 86400000, "restartConfig": map[string]any{"restartPolicy": "Never", "maxRestartAttempts": 0, "restartCounterResetMillis": -1}}
	// JSON normalization is required because API decoding uses float64 numbers/maps.
	encoded, _ := json.Marshal(spec)
	if err = json.Unmarshal(encoded, &spec); err != nil {
		return application{}, err
	}
	hash := sha256.Sum256(encoded)
	return application{APIVersion: "spark.apache.org/v1", Kind: "SparkApplication", Metadata: objectMeta{Name: r.KubernetesName, Namespace: r.Profile.Namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "dataslate", "dataslate.io/run": r.ID, "dataslate.io/workspace": r.WorkspaceID, "dataslate.io/binding": r.BindingID}, Annotations: map[string]string{"dataslate.io/spec-sha256": hex.EncodeToString(hash[:])}}, Spec: spec}, nil
}

func restrictedTemplate(container string, apiToken bool) map[string]any {
	return map[string]any{"podTemplateSpec": map[string]any{"spec": map[string]any{
		"automountServiceAccountToken": apiToken,
		"securityContext":              map[string]any{"runAsNonRoot": true, "runAsUser": 185, "runAsGroup": 185, "seccompProfile": map[string]string{"type": "RuntimeDefault"}},
		"containers":                   []any{map[string]any{"name": container, "securityContext": map[string]any{"allowPrivilegeEscalation": false, "capabilities": map[string]any{"drop": []string{"ALL"}}}}},
	}}}
}

func appPath(r platform.Run) string {
	return "/apis/spark.apache.org/v1/namespaces/" + r.Profile.Namespace + "/sparkapplications"
}

func (c *Client) get(ctx context.Context, r platform.Run) (application, bool, error) {
	expected, err := c.manifest(r)
	if err != nil {
		return application{}, false, err
	}
	var actual application
	err = c.request(ctx, http.MethodGet, appPath(r)+"/"+r.KubernetesName, nil, &actual)
	if statusIs(err, http.StatusNotFound) {
		return application{}, false, nil
	}
	if err != nil {
		return application{}, false, err
	}
	if err = verify(actual, expected, r.KubernetesUID); err != nil {
		return application{}, false, err
	}
	return actual, true, nil
}

func verify(actual, expected application, uid string) error {
	if actual.APIVersion != expected.APIVersion || actual.Kind != expected.Kind || actual.Metadata.Name != expected.Metadata.Name || actual.Metadata.Namespace != expected.Metadata.Namespace || actual.Metadata.UID == "" || len(actual.Metadata.UID) > 256 || (uid != "" && uid != actual.Metadata.UID) {
		return errors.New("SparkApplication identity mismatch; refusing resource")
	}
	for k, v := range expected.Metadata.Labels {
		if actual.Metadata.Labels[k] != v {
			return errors.New("SparkApplication ownership mismatch; refusing resource")
		}
	}
	if actual.Metadata.Annotations["dataslate.io/spec-sha256"] != expected.Metadata.Annotations["dataslate.io/spec-sha256"] || !reflect.DeepEqual(actual.Spec, expected.Spec) {
		return errors.New("SparkApplication specification changed; refusing resource")
	}
	return nil
}

func (c *Client) Observe(ctx context.Context, r platform.Run) (platform.BackendObservation, error) {
	a, exists, err := c.get(ctx, r)
	if err != nil {
		return platform.BackendObservation{}, err
	}
	if !exists {
		return platform.BackendObservation{Exists: false}, nil
	}
	return observe(a), nil
}

func observe(a application) platform.BackendObservation {
	o := platform.BackendObservation{Exists: true, UID: a.Metadata.UID, State: "unknown", Message: "Operator state has not been confirmed."}
	if a.Metadata.DeletionTimestamp != "" {
		o.Message = "Backend resource deletion is in progress."
		return o
	}
	state := a.Status.Current.Summary
	if state == "ResourceReleased" || state == "TerminatedWithoutReleaseResources" {
		// Cleanup is not success. Use the latest pre-cleanup outcome, never a message string.
		keys := []int64{}
		for key := range a.Status.History {
			if k, err := strconv.ParseInt(key, 10, 64); err == nil && k >= 0 {
				keys = append(keys, k)
			}
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] > keys[j] })
		foundOutcome := false
		for _, k := range keys {
			s := a.Status.History[strconv.FormatInt(k, 10)].Summary
			if s == "ResourceReleased" || s == "TerminatedWithoutReleaseResources" {
				continue
			}
			state = s
			foundOutcome = true
			break
		}
		if !foundOutcome {
			return o
		}
		switch state {
		case "Succeeded", "Failed", "DriverEvicted", "SchedulingFailure", "DriverStartTimedOut", "ExecutorsStartTimedOut", "DriverReadyTimedOut":
		default:
			return o
		}
	}
	switch state {
	case "", "Submitted", "DriverRequested":
		o.State = "submitting"
		o.Message = "Application accepted; waiting for the Spark driver."
	case "DriverStarted", "DriverReady", "InitializedBelowThresholdExecutors", "RunningHealthy", "RunningWithPartialCapacity", "RunningWithBelowThresholdExecutors":
		o.State = "running"
		o.Message = "The operator reports an active Spark driver."
	case "Succeeded":
		o.State = "succeeded"
		o.Message = "The operator reports successful application completion."
	case "Failed", "DriverEvicted", "SchedulingFailure", "DriverStartTimedOut", "ExecutorsStartTimedOut", "DriverReadyTimedOut":
		o.State = "failed"
		o.Message = "The operator reports application failure (" + state + ")."
	default:
		o.Message = "Operator state requires investigation; no successful result is assumed."
	}
	return o
}

func (c *Client) Submit(ctx context.Context, r platform.Run) (platform.BackendObservation, error) {
	return c.submit(ctx, r, nil)
}

func (c *Client) submit(ctx context.Context, r platform.Run, authorize func(context.Context, platform.Run) (bool, error)) (platform.BackendObservation, error) {
	if r.CancelRequested || r.Terminal() {
		return platform.BackendObservation{}, errors.New("run no longer permits submission")
	}
	if !time.Now().Before(r.ExecutionDeadline()) {
		return platform.BackendObservation{}, errors.New("run lifetime elapsed; submission is forbidden")
	}
	manifest, err := c.manifest(r)
	if err != nil {
		return platform.BackendObservation{}, err
	}
	// Observe first, including calls outside the reconciler; never patch or replace an existing object.
	existing, exists, err := c.get(ctx, r)
	if err != nil {
		return platform.BackendObservation{}, err
	}
	if exists {
		return observe(existing), nil
	}
	if r.KubernetesUID != "" {
		return platform.BackendObservation{}, errors.New("previously submitted resource is absent; resubmission refused")
	}
	if authorize != nil {
		allowed, err := authorize(ctx, r)
		if err != nil {
			return platform.BackendObservation{}, err
		}
		if !allowed {
			return platform.BackendObservation{}, errors.New("execution authorization was revoked before submission")
		}
	}
	if r.Profile.Kind() == platform.NotebookPythonBatch {
		if r.SourceArtifact == nil || r.SourceArtifact.State != "ready" || r.SourceArtifact.UID == "" {
			return platform.BackendObservation{}, errors.New("notebook source was not durably prepared")
		}
		_, exists, e := c.getSource(ctx, r)
		if e != nil || !exists {
			return platform.BackendObservation{}, errors.New("notebook source identity could not be verified before submission")
		}
	}
	var actual application
	if !time.Now().Before(r.ExecutionDeadline()) {
		return platform.BackendObservation{}, errors.New("run lifetime elapsed before POST; cancellation requires reconciliation")
	}
	err = c.request(ctx, http.MethodPost, appPath(r), manifest, &actual)
	if err == nil {
		if err = verify(actual, manifest, r.KubernetesUID); err != nil {
			return platform.BackendObservation{}, err
		}
		return observe(actual), nil
	}
	// A conflict, timeout or malformed response may have committed. Read the deterministic
	// identity once. There is no retry of POST, even when this read returns 404.
	observed, exists, getErr := c.get(ctx, r)
	if getErr == nil && exists {
		return observe(observed), nil
	}
	return platform.BackendObservation{}, errors.New("submission outcome is unconfirmed; observe before any further action")
}

func (c *Client) Cancel(ctx context.Context, r platform.Run) (platform.BackendObservation, error) {
	a, exists, err := c.get(ctx, r)
	if err != nil {
		return platform.BackendObservation{}, err
	}
	if !exists {
		return platform.BackendObservation{Exists: false, State: "cancelled", Message: "Backend resource is absent."}, nil
	}
	o := observe(a)
	if o.State == "succeeded" || o.State == "failed" {
		return o, nil
	}
	if a.Metadata.DeletionTimestamp == "" {
		input := map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground", "preconditions": map[string]string{"uid": a.Metadata.UID}}
		err = c.request(ctx, http.MethodDelete, appPath(r)+"/"+r.KubernetesName, input, nil)
		if err != nil && !statusIs(err, http.StatusNotFound) {
			return platform.BackendObservation{}, err
		}
	}
	// A DELETE acceptance isn't completion: foreground collection must finish first.
	a, exists, err = c.get(ctx, r)
	if err != nil {
		return platform.BackendObservation{}, err
	}
	if !exists {
		return platform.BackendObservation{Exists: false, State: "cancelled", Message: "Backend resource deletion confirmed."}, nil
	}
	return platform.BackendObservation{Exists: true, UID: a.Metadata.UID, State: "cancel_requested", Message: "Waiting for foreground resource deletion."}, nil
}
