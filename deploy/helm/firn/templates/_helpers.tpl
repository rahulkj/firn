{{- define "firn.name" -}}
{{- .Chart.Name }}
{{- end }}

{{- define "firn.fullname" -}}
{{- if eq .Release.Name "firn" }}{{ .Chart.Name }}{{ else }}{{ printf "%s-%s" .Release.Name .Chart.Name }}{{ end }}
{{- end }}

{{- define "firn.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/name: {{ include "firn.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "firn.selectorLabels" -}}
app.kubernetes.io/name: {{ include "firn.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "firn.replicaCount" -}}
{{- if eq .Values.mode "cluster" -}}3{{- else -}}1{{- end -}}
{{- end }}

{{- define "firn.peers" -}}
{{- $root := . -}}
{{- $peers := list -}}
{{- range $i := until (int (include "firn.replicaCount" $root)) -}}
{{- $name := printf "%s-%d" (include "firn.fullname" $root) $i -}}
{{- $peers = append $peers (printf "%s=%s.%s-mesh.%s:%d" $name $name (include "firn.fullname" $root) $root.Release.Namespace (int $root.Values.service.meshPort)) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end }}
