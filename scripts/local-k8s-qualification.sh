#!/usr/bin/env bash
# Real local Kubernetes/operator qualification, with ephemeral memory storage.
# Requires macOS, Colima, kubectl, Go, Python3, and Helm. Creates only the fixed
# disposable profile below; refuses to reuse an existing profile. No cloud calls.
# Evidence remains in .qa/local-k8s/<UTC timestamp>; credentials are removed.
# Set QUALIFICATION_MODE=notebook to qualify saved Python snapshot execution.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"
export QUALIFICATION_MODE="${QUALIFICATION_MODE:-approved}"
case "$QUALIFICATION_MODE" in approved|notebook) ;; *) echo 'QUALIFICATION_MODE must be approved or notebook' >&2; exit 1 ;; esac
PROFILE=dataslate-qualification
CONTEXT=colima-dataslate-qualification
export PATH="/opt/homebrew/bin:$PATH"
HELM="${HELM:-$ROOT/deploy/.cache/helm}"
for cmd in colima kubectl go python3; do command -v "$cmd" >/dev/null || { echo "Missing prerequisite: $cmd" >&2; exit 1; }; done
[[ -x "$HELM" ]] || { echo 'Set HELM to an installed Helm binary.' >&2; exit 1; }
[[ -f deploy/.cache/spark-operator.tgz ]] || { echo 'Missing pinned deploy/.cache/spark-operator.tgz (chart 1.8.0).' >&2; exit 1; }
colima list --json | python3 -c 'import json,sys; a=[json.loads(x) for x in sys.stdin.read().splitlines() if x.strip()]; sys.exit("Refusing to touch existing Colima profile: dataslate-qualification" if any(x.get("name")=="dataslate-qualification" for x in a) else 0)'
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$ROOT/.qa/local-k8s/$STAMP"
mkdir -p "$OUT/docker-config" "$ROOT/api/.qa/local-k8s"
chmod 700 "$OUT"
export KUBECONFIG="$OUT/kubeconfig" DOCKER_CONFIG="$OUT/docker-config"
CREATED=0
WATCH_PID=""
cleanup() {
  status=$?
  trap - EXIT
  if [[ -n "$WATCH_PID" ]]; then kill "$WATCH_PID" 2>/dev/null || true; fi
  if [[ "$CREATED" == 1 ]]; then
    colima delete "$PROFILE" --force --data > "$OUT/cleanup.log" 2>&1 || { echo "Profile cleanup failed; inspect $OUT/cleanup.log" >&2; status=1; }
  fi
  rm -f "$OUT/controller-token" "$OUT/api-token" "$OUT/workload-token" "$OUT/kubeconfig" "$OUT/config.json" "$ROOT/api/.qa/local-k8s/main.go"
  echo "Qualification evidence: $OUT"
  exit "$status"
}
trap cleanup EXIT
CREATED=1
colima start "$PROFILE" --activate=false --ssh-config=false --runtime containerd --kubernetes \
  --kubernetes-version v1.35.0+k3s1 --cpus 3 --memory 5 --disk 20 --root-disk 10 \
  --mount none --k3s-arg=--disable=traefik,servicelb,metrics-server > "$OUT/colima.log" 2>&1
chmod 600 "$KUBECONFIG"
k() { kubectl --context "$CONTEXT" "$@"; }
k get nodes -o json > "$OUT/nodes.json"
k -n default get endpoints kubernetes -o json > "$OUT/kubernetes-endpoint.json"
python3 - "$OUT" <<'PYCOMPUTE'
import ipaddress,json,os,sys
from pathlib import Path
out=Path(sys.argv[1]);ep=json.loads((out/'kubernetes-endpoint.json').read_text())['subsets'][0]
ports=[p['port'] for p in ep['ports'] if p['name']=='https'];assert len(ports)==1
cidrs=[a['ip']+('/32' if ipaddress.ip_address(a['ip']).version==4 else '/128') for a in ep['addresses']]
c={'namespace':'dataslate-qualification','serviceAccount':'spark-fixture','operatorNamespace':'dataslate-operator','operatorServiceAccount':'spark-operator','images':['apache/spark@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211'],'apiCidrs':cidrs,'apiPort':ports[0],'dnsNamespace':'kube-system','dnsPodLabels':{'k8s-app':'kube-dns'},'s3Cidrs':[],'stsCidrs':[],'irsaRoleArn':'','maxPods':8,'quotaCpu':8,'quotaMemoryGi':8}
if os.environ['QUALIFICATION_MODE']=='notebook':c['notebookSources']={'controllerNamespace':'dataslate-control-qualification','controllerServiceAccount':'dataslate-controller'}
(out/'compute-input.json').write_text(json.dumps(c,indent=2))
PYCOMPUTE
python3 deploy/compute/generate.py --config "$OUT/compute-input.json" > "$OUT/compute-boundary.json"
k apply -f "$OUT/compute-boundary.json" > "$OUT/compute-apply.log"
python3 - "$OUT" <<'PY'
import json,os,sys
from pathlib import Path
out=Path(sys.argv[1])
base={'workspaceId':'default','name':'Local qualification fixture only','operator':'apache','image':'apache/spark@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211','mainApplicationFile':'local:///opt/spark/examples/src/main/python/pi.py','namespace':'dataslate-qualification','serviceAccount':'spark-fixture','executorInstances':1,'allowedSubjects':[{'issuer':'development','subject':'development-admin'}]}
a=[]
for kind in ['pi','failure','cancel','revoke']:
    b=base|{'id':'local-'+kind+'-fixture'}
    if kind=='failure': b['mainApplicationFile']='local:///opt/spark/nonexistent-fixture.py'
    a.append(b)
if os.environ['QUALIFICATION_MODE']=='notebook':
    base.pop('mainApplicationFile');base['executionKind']='notebook-python-batch'
    a=[base|{'id':'local-notebook-fixture','maxRunSeconds':240},base|{'id':'local-notebook-timeout','maxRunSeconds':60}]
(out/'bindings.json').write_text(json.dumps(a,indent=2))
PY
cat > "$OUT/operator-values.yaml" <<'YAML'
image:
  digest: sha256:349da72f95c2b7d1882729106f7b4a659803a8b6e8fd9be35ee3b8544ba7616b
operatorDeployment:
  replicas: 1
  operatorPod:
    operatorContainer:
      jvmArgs: "-Dfile.encoding=UTF8 -XX:+UseParallelGC -XX:MaxRAMPercentage=70"
      resources:
        requests: {cpu: 200m, memory: 512Mi, ephemeral-storage: 512Mi}
        limits: {cpu: 1, memory: 768Mi, ephemeral-storage: 2Gi}
workloadResources:
  namespaces:
    create: false
    overrideWatchedNamespaces: false
  serviceAccount:
    create: false
  clusterRole:
    create: false
  role:
    create: false
  roleBinding:
    create: false
operatorConfiguration:
  spark-operator.properties: |
    spark.kubernetes.operator.watchedNamespaces=dataslate-qualification
YAML
"$HELM" show chart deploy/.cache/spark-operator.tgz > "$OUT/chart.yaml"
python3 - "$OUT/chart.yaml" <<'PY'
from pathlib import Path
import sys
s=Path(sys.argv[1]).read_text().splitlines()
assert 'version: 1.8.0' in s and 'appVersion: 1.0.0' in s, 'Unexpected upstream chart version'
PY
"$HELM" install dataslate-qualification deploy/.cache/spark-operator.tgz --kube-context "$CONTEXT" \
  --namespace dataslate-operator --create-namespace -f "$OUT/operator-values.yaml" --wait --timeout 180s > "$OUT/helm.log"
python3 - "$OUT/adapter-rbac.yaml" <<'PYRBAC'
import json,os,sys
control='dataslate-control-qualification'
items=[{'apiVersion':'v1','kind':'Namespace','metadata':{'name':control}}]
ns='dataslate-qualification'
for name,verbs in [('dataslate-controller',['get','create','delete']),('dataslate-api',['get'])]:
    rules=[{'apiGroups':['spark.apache.org'],'resources':['sparkapplications'],'verbs':verbs}]
    if name=='dataslate-controller' and os.environ['QUALIFICATION_MODE']=='notebook':rules += [{'apiGroups':[''],'resources':['configmaps'],'verbs':['get','create','patch','delete']}]
    if name=='dataslate-api':rules += [{'apiGroups':[''],'resources':['pods'],'verbs':['get','list']},{'apiGroups':[''],'resources':['pods/log'],'verbs':['get']}]
    metadata={'name':name,'namespace':ns}
    items += [{'apiVersion':'v1','kind':'ServiceAccount','metadata':{'name':name,'namespace':control}},{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'Role','metadata':metadata,'rules':rules},{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'RoleBinding','metadata':metadata,'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':name},'subjects':[{'kind':'ServiceAccount','name':name,'namespace':control}]}]
open(sys.argv[1],'w').write(json.dumps({'apiVersion':'v1','kind':'List','items':items}))
PYRBAC
k apply -f "$OUT/adapter-rbac.yaml" > "$OUT/rbac.log"
for probe in 'create pods dataslate-qualification' 'get pods dataslate-qualification' 'get pods/log dataslate-qualification' 'get secrets dataslate-qualification' 'create sparkapplications.spark.apache.org default'; do
  read -r verb resource namespace <<< "$probe"
  if k auth can-i --as=system:serviceaccount:dataslate-control-qualification:dataslate-controller "$verb" "$resource" -n "$namespace" >> "$OUT/rbac-denials.log"; then
    echo "Unexpected RBAC allowance: $probe" >&2; exit 1
  fi
done
if k auth can-i --as=system:serviceaccount:dataslate-control-qualification:dataslate-api create sparkapplications.spark.apache.org -n dataslate-qualification >> "$OUT/rbac-denials.log"; then
  echo "Unexpected API submission privilege" >&2; exit 1
fi
k -n dataslate-control-qualification create token dataslate-controller --duration=30m > "$OUT/controller-token"
k -n dataslate-control-qualification create token dataslate-api --duration=30m > "$OUT/api-token"
if [[ "$QUALIFICATION_MODE" == notebook ]]; then
  k -n dataslate-qualification create token spark-fixture --duration=30m > "$OUT/workload-token"
  chmod 600 "$OUT/workload-token"
fi
k config view --raw --minify -o json > "$OUT/config.json"
python3 - "$OUT" <<'PY'
import base64,json,os,sys
from pathlib import Path
out=Path(sys.argv[1]); c=json.loads((out/'config.json').read_text())['clusters'][0]['cluster']
(out/'ca.crt').write_bytes(base64.b64decode(c['certificate-authority-data']))
(out/'server-url').write_text(c['server'])
for name in ['config.json','controller-token','api-token','bindings.json']:os.chmod(out/name,0o600)
PY
python3 deploy/compute/verify_admission.py --context "$CONTEXT" --config "$OUT/compute-input.json" > "$OUT/admission-verifier.json"
if [[ "$QUALIFICATION_MODE" == notebook ]]; then
  python3 scripts/local-k8s-qualification.sources.py --context "$CONTEXT" --config "$OUT/compute-input.json" > "$OUT/source-admission-verifier.json"
fi
k -n dataslate-qualification get pods --watch --output-watch-events -o json > "$OUT/pod-watch-events.json-stream" 2> "$OUT/pod-watch-errors.txt" &
WATCH_PID=$!
if [[ "$QUALIFICATION_MODE" == notebook ]]; then
  cp scripts/local-k8s-qualification.notebook.go api/.qa/local-k8s/main.go
else
  cp scripts/local-k8s-qualification.go api/.qa/local-k8s/main.go
fi
export DATASLATE_EXECUTION_BINDINGS_FILE="$OUT/bindings.json"
export DATASLATE_KUBERNETES_API_URL="$(cat "$OUT/server-url")"
export DATASLATE_KUBERNETES_CA_FILE="$OUT/ca.crt"
export DATASLATE_KUBERNETES_TOKEN_FILE="$OUT/controller-token"
export DATASLATE_KUBERNETES_API_TOKEN_FILE="$OUT/api-token"
export DATASLATE_EXECUTION_NAMESPACES=dataslate-qualification
export QUALIFICATION_WORKLOAD_TOKEN_FILE="$OUT/workload-token"
export QUALIFICATION_NOTEBOOK_FILE="$ROOT/examples/python-batch-notebook.json"
export QUALIFICATION_OUTPUT_DIR="$OUT"
(cd api && go run ./.qa/local-k8s) | tee "$OUT/harness.log"
k -n dataslate-qualification get sparkapplications,pods -o json > "$OUT/final-resources.json"
k -n dataslate-operator logs deploy/spark-kubernetes-operator > "$OUT/operator.log"
