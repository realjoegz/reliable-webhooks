{{/*
The name of every resource of a release: the release name, with the chart name behind it unless
the release name already holds it.
*/}}
{{- define "reliable-webhooks.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
The labels that select the replicas of a release. A Deployment cannot change its selector, so
nothing that changes from one release to the next belongs here.
*/}}
{{- define "reliable-webhooks.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "reliable-webhooks.labels" -}}
{{ include "reliable-webhooks.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}
