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
