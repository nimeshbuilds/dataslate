package platform

import (
	"errors"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/distribution/reference"
)

type SparkInput struct {
	Name                string `json:"name"`
	Operator            string `json:"operator"`
	Image               string `json:"image"`
	MainApplicationFile string `json:"mainApplicationFile"`
	ServiceAccount      string `json:"serviceAccount"`
	Namespace           string `json:"namespace"`
	ExecutorInstances   int    `json:"executorInstances"`
}
type SparkPreview struct {
	Manifest  map[string]any `json:"manifest"`
	Warnings  []string       `json:"warnings"`
	Submitted bool           `json:"submitted"`
}

func CompileSpark(in SparkInput, workspaceID string) (SparkPreview, error) {
	out := SparkPreview{Warnings: []string{"Preview only: no Kubernetes request was sent.", "Runtime candidate is Spark 4.1.3. The image, Python, Scala, Delta and operator versions require compatibility qualification.", "Namespace, service account, image policy, storage identity and admission restrictions must be authorized before any future submission.", "Only Python .py applications are supported by this preview API. Executor count is fixed; no dynamic allocation."}}
	if !dnsLabel.MatchString(in.Name) || !dnsLabel.MatchString(in.Namespace) || !dnsLabel.MatchString(in.ServiceAccount) {
		return out, errors.New("name, namespace and serviceAccount must be Kubernetes DNS labels")
	}
	if in.Operator != "apache" && in.Operator != "kubeflow" {
		return out, errors.New("operator must be apache or kubeflow")
	}
	image, err := reference.ParseNormalizedNamed(in.Image)
	if err != nil || len(in.Image) > 512 {
		return out, errors.New("image must be a valid OCI image reference")
	}
	if _, tagged := image.(reference.NamedTagged); !tagged {
		if _, digested := image.(reference.Digested); !digested {
			return out, errors.New("image must specify an explicit version tag or digest")
		}
	}
	if tagged, ok := image.(reference.NamedTagged); ok && tagged.Tag() == "latest" {
		return out, errors.New("mutable latest image tag is not accepted")
	}
	u, err := url.Parse(in.MainApplicationFile)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || len(in.MainApplicationFile) > 2048 || strings.ContainsAny(in.MainApplicationFile, "\r\n\t ,") || path.Ext(u.Path) != ".py" {
		return out, errors.New("mainApplicationFile must be a .py URI without credentials, query, fragment or separators")
	}
	switch u.Scheme {
	case "local":
		if u.Host != "" || !strings.HasPrefix(u.Path, "/") {
			return out, errors.New("local application URI must use local:///absolute/path.py")
		}
	case "s3", "s3a":
		if u.Host == "" || u.Path == "" {
			return out, errors.New("S3 application URI requires a bucket and path")
		}
	default:
		return out, errors.New("application URI scheme must be local, s3 or s3a for the AWS profile")
	}
	if path.Clean(u.Path) != u.Path || strings.Contains(u.Path, "\\") {
		return out, errors.New("application path must be canonical without traversal")
	}
	if in.ExecutorInstances < 1 || in.ExecutorInstances > 100 {
		return out, errors.New("executorInstances must be between 1 and 100")
	}
	manifest := map[string]any{"kind": "SparkApplication", "metadata": map[string]any{"name": in.Name, "namespace": in.Namespace, "labels": map[string]string{"app.kubernetes.io/managed-by": "dataslate", "dataslate.io/workspace": workspaceID}}}
	if in.Operator == "apache" {
		manifest["apiVersion"] = "spark.apache.org/v1"
		manifest["spec"] = map[string]any{"pyFiles": in.MainApplicationFile, "runtimeVersions": map[string]string{"sparkVersion": "4.1.3"}, "sparkConf": map[string]string{"spark.kubernetes.container.image": in.Image, "spark.kubernetes.authenticate.driver.serviceAccountName": in.ServiceAccount, "spark.executor.instances": strconv.Itoa(in.ExecutorInstances), "spark.dynamicAllocation.enabled": "false", "spark.driver.memory": "1g", "spark.executor.memory": "1g"}, "applicationTolerations": map[string]string{"resourceRetainPolicy": "OnFailure"}}
	} else {
		manifest["apiVersion"] = "sparkoperator.k8s.io/v1beta2"
		manifest["spec"] = map[string]any{"type": "Python", "mode": "cluster", "sparkVersion": "4.1.3", "image": in.Image, "mainApplicationFile": in.MainApplicationFile, "restartPolicy": map[string]string{"type": "Never"}, "sparkConf": map[string]string{"spark.dynamicAllocation.enabled": "false"}, "driver": map[string]any{"cores": 1, "memory": "1g", "serviceAccount": in.ServiceAccount}, "executor": map[string]any{"cores": 1, "memory": "1g", "instances": in.ExecutorInstances}}
	}
	out.Manifest = manifest
	return out, nil
}
