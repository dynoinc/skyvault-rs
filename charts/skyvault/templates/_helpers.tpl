{{/*
Expand the name of the chart.
*/}}
{{- define "skyvault.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "skyvault.fullname" -}}
{{- printf "%s-%s" .Chart.Name .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "skyvault.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "skyvault.labels" -}}
helm.sh/chart: {{ include "skyvault.chart" . }}
{{ include "skyvault.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "skyvault.selectorLabels" -}}
app.kubernetes.io/name: {{ include "skyvault.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "skyvault.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "skyvault.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Get deployment instance selector labels
*/}}
{{- define "skyvault.instanceSelectorLabels" -}}
{{- include "skyvault.selectorLabels" . }}
app.kubernetes.io/instance-id: {{ .instanceName }}
{{- end }} 