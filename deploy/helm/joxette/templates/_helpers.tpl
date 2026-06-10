{{/*
Chart name / fullname helpers.
*/}}
{{- define "joxette.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "joxette.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "joxette.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels. `app.kubernetes.io/name` is the discovery anchor: it must match the
pod-label-selector used by kubernetes-api discovery in pekko-management mode.
*/}}
{{- define "joxette.labels" -}}
helm.sh/chart: {{ include "joxette.chart" . }}
{{ include "joxette.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "joxette.selectorLabels" -}}
app.kubernetes.io/name: {{ include "joxette.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "joxette.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "joxette.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Image reference, defaulting the tag to .Chart.AppVersion.
*/}}
{{- define "joxette.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Booleans derived from values, used across templates.
*/}}
{{- define "joxette.embedded" -}}
{{- eq .Values.catalog.backend "embedded" -}}
{{- end -}}

{{- define "joxette.pekkoManagement" -}}
{{- eq .Values.clustering.mode "pekko-management" -}}
{{- end -}}

{{/*
catalog.path passed to JOXETTE_CATALOG_PATH. For embedded it is the local file;
for shared backends it is the connection URI.
*/}}
{{- define "joxette.catalogPath" -}}
{{- if eq .Values.catalog.backend "embedded" -}}
{{- .Values.catalog.embedded.path -}}
{{- else -}}
{{- required "catalog.uri is required when catalog.backend is quack/postgresql" .Values.catalog.uri -}}
{{- end -}}
{{- end -}}

{{/*
==============================================================================
Guardrail — fail the render on an unsafe combination. The embedded DuckDB
catalog is single-writer per file; a second pod (or a split tier) corrupts it.
This mirrors operator-design.md §5 in chart form.
==============================================================================
*/}}
{{- define "joxette.validate" -}}
{{- if eq .Values.catalog.backend "embedded" -}}
  {{- range $name, $tier := .Values.tiers -}}
    {{- if and $tier.enabled (gt (int (default 1 $tier.replicas)) 1) -}}
      {{- fail (printf "catalog.backend=embedded is single-writer: tiers.%s.replicas=%d > 1 would corrupt the catalog file. Use a quack/postgresql backend to scale out, or set replicas to 1." $name (int $tier.replicas)) -}}
    {{- end -}}
  {{- end -}}
{{- else if not .Values.catalog.uri -}}
  {{- fail (printf "catalog.backend=%s requires catalog.uri (quack://… or postgresql://…)" .Values.catalog.backend) -}}
{{- end -}}
{{- if and (eq .Values.clustering.mode "pekko-management") (eq .Values.catalog.backend "embedded") -}}
  {{- fail "clustering.mode=pekko-management forms a multi-pod cluster, which is incompatible with the single-writer embedded catalog. Use a quack/postgresql backend, or clustering.mode=catalog." -}}
{{- end -}}
{{- if and .Values.tiers.replay.hpa.enabled (eq .Values.catalog.backend "embedded") -}}
  {{- fail "tiers.replay.hpa.enabled requires a shared (quack/postgresql) catalog: the embedded catalog runs a single all-in-one pod with no separate replay tier to scale." -}}
{{- end -}}
{{- end -}}

{{/*
Shared environment block for all pods. `roles` and `replayEnabled` vary per tier
(or are [all] for the embedded all-in-one), so they are passed in via a dict:
  include "joxette.env" (dict "ctx" $ "roles" (list "recorder") "replayEnabled" false)
*/}}
{{- define "joxette.env" -}}
{{- $ctx := .ctx -}}
- name: JOXETTE_CATALOG_PATH
  value: {{ include "joxette.catalogPath" $ctx | quote }}
- name: JOXETTE_CATALOG_OBJECT-STORAGE-PATH
  value: {{ $ctx.Values.catalog.objectStoragePath | quote }}
- name: JOXETTE_ROLES
  value: {{ join "," .roles | quote }}
- name: JOXETTE_REPLAY_ENABLED
  value: {{ .replayEnabled | quote }}
- name: JOXETTE_CLUSTERING_MODE
  value: {{ $ctx.Values.clustering.mode | quote }}
- name: JOXETTE_KAFKA_BOOTSTRAP-SERVERS
  value: {{ $ctx.Values.kafka.bootstrapServers | quote }}
- name: JOXETTE_KAFKA_CONSUMER-GROUP
  value: {{ $ctx.Values.kafka.consumerGroup | quote }}
- name: JOXETTE_KAFKA_GROUP-PROTOCOL
  value: {{ $ctx.Values.kafka.groupProtocol | quote }}
{{- if eq $ctx.Values.clustering.mode "pekko-management" }}
- name: JOXETTE_CLUSTERING_MANAGEMENT-PORT
  value: {{ $ctx.Values.clustering.managementPort | quote }}
- name: JOXETTE_CLUSTERING_SERVICE-NAME
  value: {{ include "joxette.name" $ctx | quote }}
- name: JOXETTE_CLUSTERING_REQUIRED-CONTACT-POINT-NR
  value: {{ default (int $ctx.Values.tiers.recorder.replicas) $ctx.Values.clustering.requiredContactPointNr | quote }}
- name: POD_IP
  valueFrom:
    fieldRef:
      fieldPath: status.podIP
{{- end }}
{{- if $ctx.Values.objectStore.endpoint }}
- name: JOXETTE_S3_ENDPOINT
  value: {{ $ctx.Values.objectStore.endpoint | quote }}
{{- end }}
- name: JOXETTE_S3_REGION
  value: {{ $ctx.Values.objectStore.region | quote }}
{{- if $ctx.Values.objectStore.existingSecret }}
- name: JOXETTE_S3_ACCESS-KEY
  valueFrom:
    secretKeyRef:
      name: {{ $ctx.Values.objectStore.existingSecret }}
      key: access-key
- name: JOXETTE_S3_SECRET-KEY
  valueFrom:
    secretKeyRef:
      name: {{ $ctx.Values.objectStore.existingSecret }}
      key: secret-key
{{- end }}
{{- range $k, $v := $ctx.Values.extraEnv }}
- name: {{ $k }}
  value: {{ $v | quote }}
{{- end }}
{{- end -}}

{{/*
Container ports — HTTP always; management port only in pekko-management mode.
*/}}
{{- define "joxette.containerPorts" -}}
- name: http
  containerPort: {{ .Values.service.port }}
  protocol: TCP
{{- if eq .Values.clustering.mode "pekko-management" }}
- name: management
  containerPort: {{ .Values.clustering.managementPort }}
  protocol: TCP
{{- end }}
{{- end -}}

{{/*
Probes block (shared by StatefulSet and Deployments).
*/}}
{{- define "joxette.probes" -}}
startupProbe:
  httpGet:
    path: /actuator/health
    port: http
  failureThreshold: {{ .Values.probes.startupFailureThreshold }}
  periodSeconds: 5
livenessProbe:
  httpGet:
    path: {{ .Values.probes.liveness.path }}
    port: http
  periodSeconds: 15
readinessProbe:
  httpGet:
    path: {{ .Values.probes.readiness.path }}
    port: http
  periodSeconds: 10
{{- end -}}
