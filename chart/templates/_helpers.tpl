{{- define "multigres.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "multigres.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "multigres.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "multigres.labels" -}}
app.kubernetes.io/name: {{ include "multigres.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "multigres.selectorLabels" -}}
app.kubernetes.io/name: {{ include "multigres.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "multigres.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end }}

{{- define "multigres.imagePullSecrets" -}}
{{- if .Values.image.pullSecrets }}
imagePullSecrets:
{{- range .Values.image.pullSecrets }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end }}

{{- define "multigres.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "multigres.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "multigres.etcdInitialCluster" -}}
{{- $fullname := include "multigres.fullname" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $peerPort := .Values.etcd.peerPort -}}
{{- $replicas := int .Values.etcd.replicas -}}
{{- $members := list -}}
{{- range $i := until $replicas -}}
{{- $members = append $members (printf "%s-etcd-%d=http://%s-etcd-%d.%s-etcd-headless.%s.svc.cluster.local:%d" $fullname $i $fullname $i $fullname $namespace $peerPort) -}}
{{- end -}}
{{- join "," $members -}}
{{- end }}

{{- define "multigres.etcdEndpoints" -}}
{{- $fullname := include "multigres.fullname" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $clientPort := .Values.etcd.clientPort -}}
{{- $replicas := int .Values.etcd.replicas -}}
{{- $endpoints := list -}}
{{- range $i := until $replicas -}}
{{- $endpoints = append $endpoints (printf "%s-etcd-%d.%s-etcd-headless.%s.svc.cluster.local:%d" $fullname $i $fullname $namespace $clientPort) -}}
{{- end -}}
{{- join "," $endpoints -}}
{{- end }}

{{/*
Topology server addresses - uses deployed etcd if enabled, otherwise uses configured address
*/}}
{{- define "multigres.topoServerAddresses" -}}
{{- if .Values.etcd.enabled -}}
{{- printf "%s-etcd.%s.svc.cluster.local:%d" (include "multigres.fullname" .) .Release.Namespace (int .Values.etcd.clientPort) -}}
{{- else -}}
{{- required "topology.serverAddresses is required when etcd.enabled is false" .Values.topology.serverAddresses -}}
{{- end -}}
{{- end }}
