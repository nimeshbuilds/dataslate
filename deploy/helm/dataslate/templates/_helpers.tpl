{{- define "dataslate.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 40 | trimSuffix "-" -}}
{{- end -}}
{{- define "dataslate.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 40 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "dataslate.name" .) | trunc 40 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- define "dataslate.labels" -}}
app.kubernetes.io/name: {{ include "dataslate.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end -}}
{{- define "dataslate.selector" -}}
app.kubernetes.io/name: {{ include "dataslate.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "dataslate.image" -}}
{{- if .digest -}}
{{- printf "%s@%s" .repository .digest -}}
{{- else -}}
{{- printf "%s:%s" .repository .tag -}}
{{- end -}}
{{- end -}}
{{- define "dataslate.postgresName" -}}
{{- default (printf "%s-db" (include "dataslate.fullname" .)) .Values.postgres.managed.name -}}
{{- end -}}
{{- define "dataslate.databaseSecretName" -}}
{{- if eq .Values.postgres.mode "managed" -}}
{{- printf "%s-app" (include "dataslate.postgresName" .) -}}
{{- else -}}
{{- .Values.postgres.external.secretName -}}
{{- end -}}
{{- end -}}
{{- define "dataslate.caSecretName" -}}
{{- if eq .Values.postgres.mode "managed" -}}
{{- printf "%s-ca" (include "dataslate.postgresName" .) -}}
{{- else -}}
{{- .Values.postgres.external.caSecretName -}}
{{- end -}}
{{- end -}}
{{- define "dataslate.databaseEnv" -}}
- name: DATASLATE_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "dataslate.databaseSecretName" . }}
      key: {{ ternary "uri" .Values.postgres.external.secretKey (eq .Values.postgres.mode "managed") }}
- name: PGSSLMODE
  value: verify-full
- name: PGSSLROOTCERT
  value: /etc/dataslate/postgres/ca.crt
{{- end -}}
{{- define "dataslate.databaseVolume" -}}
- name: postgres-ca
  secret:
    secretName: {{ include "dataslate.caSecretName" . }}
    defaultMode: 0444
    items:
      - key: {{ ternary "ca.crt" .Values.postgres.external.caSecretKey (eq .Values.postgres.mode "managed") }}
        path: ca.crt
{{- end -}}
{{- define "dataslate.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
capabilities:
  drop: [ALL]
{{- end -}}
{{- define "dataslate.podSecurityContext" -}}
runAsNonRoot: true
seccompProfile:
  type: RuntimeDefault
{{- end -}}
{{- define "dataslate.scheduling" -}}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{- define "dataslate.executionNamespaces" -}}
{{- .Values.execution.namespaces | sortAlpha | join " " -}}
{{- end -}}

{{- define "dataslate.sqlBindings" -}}
{{- $credentials := dict -}}
{{- range .Values.sql.credentials -}}
{{- $_ := set $credentials .id . -}}
{{- end -}}

{{- $bindings := list -}}
{{- range .Values.sql.bindings -}}
{{- $credential := get $credentials .credentialsId -}}
{{- if not $credential -}}{{- fail "SQL binding credentialsId must reference an entry in sql.credentials" -}}{{- end -}}
{{- $binding := omit . "credentialsId" -}}
{{- $_ := set $binding "tokenFile" (printf "/etc/dataslate/sql-credentials/%s/token" .credentialsId) -}}
{{- if $credential.caSecret -}}
{{- $_ := set $binding "caFile" (printf "/etc/dataslate/sql-credentials/%s/ca.crt" .credentialsId) -}}
{{- end -}}
{{- $bindings = append $bindings $binding -}}
{{- end -}}
{{- $bindings | toJson -}}
{{- end -}}

{{- define "dataslate.catalogBindings" -}}
{{- $credentials := dict -}}
{{- range .Values.catalog.credentials -}}{{- $_ := set $credentials .id . -}}{{- end -}}
{{- $bindings := list -}}
{{- range .Values.catalog.bindings -}}
{{- $credential := get $credentials .connection.credentialsId -}}
{{- if not $credential -}}{{- fail "Catalog binding credentialsId must reference catalog.credentials" -}}{{- end -}}
{{- $binding := omit . "connection" -}}
{{- $connection := omit .connection "credentialsId" -}}
{{- $prefix := printf "/etc/dataslate/catalog-credentials/%s" .connection.credentialsId -}}
{{- $auth := dict -}}
{{- if $credential.bearerTokenSecret -}}
{{- $_ := set $auth "bearerTokenFile" (printf "%s/token" $prefix) -}}
{{- else -}}
{{- $auth = omit $credential.oauth "clientSecret" -}}
{{- $_ := set $auth "clientSecretFile" (printf "%s/client-secret" $prefix) -}}
{{- end -}}
{{- $_ := set $connection "auth" $auth -}}
{{- if $credential.caSecret -}}{{- $_ := set $connection "caFile" (printf "%s/ca.crt" $prefix) -}}{{- end -}}
{{- $_ := set $binding "connection" $connection -}}
{{- $bindings = append $bindings $binding -}}
{{- end -}}
{{- $bindings | toJson -}}
{{- end -}}

{{- define "dataslate.searchBindings" -}}
{{- $credentials := dict -}}
{{- range .Values.search.credentials -}}{{- $_ := set $credentials .id . -}}{{- end -}}
{{- $bindings := list -}}
{{- range .Values.search.bindings -}}
{{- $binding := omit . "embedding" -}}
{{- if .embedding -}}
{{- $credential := get $credentials .embedding.credentialsId -}}
{{- if not $credential -}}{{- fail "Search embedding credentialsId must reference search.credentials" -}}{{- end -}}
{{- $embedding := omit .embedding "credentialsId" -}}
{{- $prefix := printf "/etc/dataslate/search-credentials/%s" .embedding.credentialsId -}}
{{- $auth := dict -}}
{{- if $credential.bearerTokenSecret -}}
{{- $_ := set $auth "bearerTokenFile" (printf "%s/token" $prefix) -}}
{{- else -}}
{{- $auth = omit $credential.oauth "clientSecret" -}}
{{- $_ := set $auth "clientSecretFile" (printf "%s/client-secret" $prefix) -}}
{{- end -}}
{{- $_ := set $embedding "auth" $auth -}}
{{- if $credential.caSecret -}}{{- $_ := set $embedding "caFile" (printf "%s/ca.crt" $prefix) -}}{{- end -}}
{{- $_ := set $binding "embedding" $embedding -}}
{{- end -}}
{{- $bindings = append $bindings $binding -}}
{{- end -}}
{{- $bindings | toJson -}}
{{- end -}}

{{/* Preserve catalog binding hashes while limiting the worker to referenced connections. */}}
{{- define "dataslate.searchCatalogBindings" -}}
{{- $wanted := dict -}}
{{- range .Values.search.bindings -}}
{{- $workspace := .workspaceId -}}
{{- range .sources.catalogs -}}{{- $_ := set $wanted (printf "%s/%s" $workspace .bindingId) true -}}{{- end -}}
{{- end -}}
{{- $bindings := list -}}
{{- range (include "dataslate.catalogBindings" . | fromJsonArray) -}}
{{- if hasKey $wanted (printf "%s/%s" .workspaceId .id) -}}{{- $bindings = append $bindings . -}}{{- end -}}
{{- end -}}
{{- $bindings | toJson -}}
{{- end -}}

{{- define "dataslate.searchCatalogCredentials" -}}
{{- $wanted := dict -}}
{{- range .Values.search.bindings -}}
{{- $workspace := .workspaceId -}}
{{- range .sources.catalogs -}}{{- $_ := set $wanted (printf "%s/%s" $workspace .bindingId) true -}}{{- end -}}
{{- end -}}
{{- $ids := dict -}}
{{- range .Values.catalog.bindings -}}{{- if hasKey $wanted (printf "%s/%s" .workspaceId .id) -}}{{- $_ := set $ids .connection.credentialsId true -}}{{- end -}}{{- end -}}
{{- $credentials := list -}}
{{- range .Values.catalog.credentials -}}{{- if hasKey $ids .id -}}{{- $credentials = append $credentials . -}}{{- end -}}{{- end -}}
{{- $credentials | toJson -}}
{{- end -}}

{{/* Only named Secret keys are projected. Values and arbitrary file paths are never accepted. */}}
{{- define "dataslate.searchCredentialMounts" -}}
{{- range . }}
- name: search-credential-{{ .id }}
  mountPath: {{ printf "/etc/dataslate/search-credentials/%s" .id | quote }}
  readOnly: true
{{- end }}
{{- end -}}
{{- define "dataslate.searchCredentialVolumes" -}}
{{- range . }}
- name: search-credential-{{ .id }}
  projected:
    defaultMode: 0440
    sources:
      {{- if .bearerTokenSecret }}
      - secret:
          name: {{ .bearerTokenSecret.name | quote }}
          items: [{key: {{ .bearerTokenSecret.key | quote }}, path: token}]
      {{- else }}
      - secret:
          name: {{ .oauth.clientSecret.name | quote }}
          items: [{key: {{ .oauth.clientSecret.key | quote }}, path: client-secret}]
      {{- end }}
      {{- if .caSecret }}
      - secret:
          name: {{ .caSecret.name | quote }}
          items: [{key: {{ .caSecret.key | quote }}, path: ca.crt}]
      {{- end }}
{{- end }}
{{- end -}}

{{- define "dataslate.sylonBindings" -}}
{{- $credentials := dict -}}
{{- range .Values.sylon.credentials -}}{{- $_ := set $credentials .id . -}}{{- end -}}
{{- $bindings := list -}}
{{- range .Values.sylon.bindings -}}
{{- $binding := omit . "provider" -}}
{{- if .provider -}}
{{- $credential := get $credentials .provider.credentialsId -}}
{{- if not $credential -}}{{- fail "Sylon provider credentialsId must reference sylon.credentials" -}}{{- end -}}
{{- $provider := omit .provider "credentialsId" -}}
{{- $prefix := printf "/etc/dataslate/sylon-credentials/%s" .provider.credentialsId -}}
{{- $auth := dict -}}
{{- if $credential.bearerTokenSecret -}}
{{- $_ := set $auth "bearerTokenFile" (printf "%s/token" $prefix) -}}
{{- else -}}
{{- $auth = omit $credential.oauth "clientSecret" -}}
{{- $_ := set $auth "clientSecretFile" (printf "%s/client-secret" $prefix) -}}
{{- end -}}
{{- $_ := set $provider "auth" $auth -}}
{{- if $credential.caSecret -}}{{- $_ := set $provider "caFile" (printf "%s/ca.crt" $prefix) -}}{{- end -}}
{{- $_ := set $binding "provider" $provider -}}
{{- end -}}
{{- $bindings = append $bindings $binding -}}
{{- end -}}
{{- $bindings | toJson -}}
{{- end -}}


{{- define "dataslate.sylonCredentialMounts" -}}
{{- range . }}
- name: sylon-credential-{{ .id }}
  mountPath: {{ printf "/etc/dataslate/sylon-credentials/%s" .id | quote }}
  readOnly: true
{{- end }}
{{- end -}}
{{- define "dataslate.sylonCredentialVolumes" -}}
{{- range . }}
- name: sylon-credential-{{ .id }}
  projected:
    defaultMode: 0440
    sources:
      {{- if .bearerTokenSecret }}
      - secret:
          name: {{ .bearerTokenSecret.name | quote }}
          items: [{key: {{ .bearerTokenSecret.key | quote }}, path: token}]
      {{- else }}
      - secret:
          name: {{ .oauth.clientSecret.name | quote }}
          items: [{key: {{ .oauth.clientSecret.key | quote }}, path: client-secret}]
      {{- end }}
      {{- if .caSecret }}
      - secret:
          name: {{ .caSecret.name | quote }}
          items: [{key: {{ .caSecret.key | quote }}, path: ca.crt}]
      {{- end }}
{{- end }}
{{- end -}}


{{- define "dataslate.sylonSearchBindings" -}}
{{- $wanted := dict -}}
{{- range .Values.sylon.bindings -}}
{{- $workspace := .workspaceId -}}{{- range .context.searchBindingIds -}}{{- $_ := set $wanted (printf "%s/%s" $workspace .) true -}}{{- end -}}
{{- end -}}
{{- $out := list -}}
{{- range (include "dataslate.searchBindings" . | fromJsonArray) -}}{{- if hasKey $wanted (printf "%s/%s" .workspaceId .id) -}}{{- $out = append $out . -}}{{- end -}}{{- end -}}
{{- $out | toJson -}}
{{- end -}}

{{- define "dataslate.sylonCatalogBindings" -}}
{{- $wanted := dict -}}
{{- range .Values.sylon.bindings -}}
{{- $workspace := .workspaceId -}}{{- range .context.catalogBindingIds -}}{{- $_ := set $wanted (printf "%s/%s" $workspace .) true -}}{{- end -}}
{{- end -}}
{{/* Search context also needs current catalog grant hashes to filter retrieval. */}}
{{- range (include "dataslate.sylonSearchBindings" . | fromJsonArray) -}}
{{- $workspace := .workspaceId -}}{{- range .sources.catalogs -}}{{- $_ := set $wanted (printf "%s/%s" $workspace .bindingId) true -}}{{- end -}}
{{- end -}}
{{- $out := list -}}
{{- range (include "dataslate.catalogBindings" . | fromJsonArray) -}}{{- if hasKey $wanted (printf "%s/%s" .workspaceId .id) -}}{{- $out = append $out . -}}{{- end -}}{{- end -}}
{{- $out | toJson -}}
{{- end -}}

{{- define "dataslate.sylonSearchCredentials" -}}
{{- $wanted := dict -}}{{- range (include "dataslate.sylonSearchBindings" . | fromJsonArray) -}}{{- $_ := set $wanted (printf "%s/%s" .workspaceId .id) true -}}{{- end -}}
{{- $ids := dict -}}{{- range .Values.search.bindings -}}{{- if and (hasKey $wanted (printf "%s/%s" .workspaceId .id)) .embedding -}}{{- $_ := set $ids .embedding.credentialsId true -}}{{- end -}}{{- end -}}
{{- $out := list -}}{{- range .Values.search.credentials -}}{{- if hasKey $ids .id -}}{{- $out = append $out . -}}{{- end -}}{{- end -}}
{{- $out | toJson -}}
{{- end -}}
{{- define "dataslate.sylonCatalogCredentials" -}}
{{- $wanted := dict -}}{{- range (include "dataslate.sylonCatalogBindings" . | fromJsonArray) -}}{{- $_ := set $wanted (printf "%s/%s" .workspaceId .id) true -}}{{- end -}}
{{- $ids := dict -}}{{- range .Values.catalog.bindings -}}{{- if hasKey $wanted (printf "%s/%s" .workspaceId .id) -}}{{- $_ := set $ids .connection.credentialsId true -}}{{- end -}}{{- end -}}
{{- $out := list -}}{{- range .Values.catalog.credentials -}}{{- if hasKey $ids .id -}}{{- $out = append $out . -}}{{- end -}}{{- end -}}
{{- $out | toJson -}}
{{- end -}}

{{- define "dataslate.publicationBindings" -}}
{{- $credentials := dict -}}
{{- range .Values.publication.credentials -}}
{{- $_ := set $credentials .id . -}}
{{- end -}}

{{- $bindings := list -}}
{{- range .Values.publication.bindings -}}
{{- $credential := get $credentials .credentialsId -}}
{{- if not $credential -}}{{- fail "Publication binding credentialsId must reference an entry in publication.credentials" -}}{{- end -}}
{{- $binding := omit . "credentialsId" -}}
{{- $_ := set $binding "tokenFile" (printf "/etc/dataslate/publication-credentials/%s/token" .credentialsId) -}}
{{- if $credential.caSecret -}}
{{- $_ := set $binding "caFile" (printf "/etc/dataslate/publication-credentials/%s/ca.crt" .credentialsId) -}}
{{- end -}}
{{- $bindings = append $bindings $binding -}}
{{- end -}}
{{- $bindings | toJson -}}
{{- end -}}

{{- define "dataslate.workflowBindings" -}}
{{- $credentials := dict -}}
{{- range .Values.workflow.credentials -}}
{{- $_ := set $credentials .id . -}}
{{- end -}}

{{- $bindings := list -}}
{{- range .Values.workflow.bindings -}}
{{- $credential := get $credentials .credentialsId -}}
{{- if not $credential -}}{{- fail "Workflow binding credentialsId must reference an entry in workflow.credentials" -}}{{- end -}}
{{- $binding := omit . "credentialsId" -}}
{{- $_ := set $binding "tokenFile" (printf "/etc/dataslate/workflow-credentials/%s/token" .credentialsId) -}}
{{- if $credential.caSecret -}}
{{- $_ := set $binding "caFile" (printf "/etc/dataslate/workflow-credentials/%s/ca.crt" .credentialsId) -}}
{{- end -}}
{{- $bindings = append $bindings $binding -}}
{{- end -}}
{{- $bindings | toJson -}}
{{- end -}}

{{- define "dataslate.workflowCredentialMounts" -}}
{{- range . }}
- name: workflow-credential-{{ .id }}
  mountPath: {{ printf "/etc/dataslate/workflow-credentials/%s" .id | quote }}
  readOnly: true
{{- end }}
{{- end -}}
{{- define "dataslate.workflowCredentialVolumes" -}}
{{- $mode := .mode -}}
{{- range .credentials }}
{{- $token := .readTokenSecret -}}{{- if eq $mode "trigger" -}}{{- $token = .triggerTokenSecret -}}{{- end }}
- name: workflow-credential-{{ .id }}
  projected:
    defaultMode: 0440
    sources:
      - secret:
          name: {{ $token.name | quote }}
          items: [{key: {{ $token.key | quote }}, path: token}]
      {{- if .caSecret }}
      - secret:
          name: {{ .caSecret.name | quote }}
          items: [{key: {{ .caSecret.key | quote }}, path: ca.crt}]
      {{- end }}
{{- end }}
{{- end -}}
