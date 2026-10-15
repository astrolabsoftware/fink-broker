{{/*
Expand the name of the chart.
*/}}
{{- define "fink.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "fink.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "fink.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "fink.labels" -}}
helm.sh/chart: {{ include "fink.chart" . }}
{{ include "fink.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "fink.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fink.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Generate s3 configuration */}}
{{- define "fink.s3config" -}}
{{ if eq .Values.storage "s3" -}}
spark.hadoop.fs.s3a.endpoint: {{ .Values.s3.endpoint }}
spark.hadoop.fs.s3a.access.key: {{ .Values.s3.access_key }}
spark.hadoop.fs.s3a.secret.key: {{ .Values.s3.secret_key }}
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version: "2"
spark.hadoop.fs.s3a.connection.ssl.enabled: "{{ .Values.s3.use_ssl }}"
spark.hadoop.fs.s3a.fast.upload: "true"
spark.hadoop.fs.s3a.path.style.access: "true"
spark.hadoop.fs.s3a.aws.credentials.provider: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
{{- end }}
{{- end }}

{{/*
Spark configuration: the S3 connector settings, or the HDFS client
replication. HDFS clients pick the replication of the files they create
from their own configuration (default 3), not from the cluster, so
hdfs.replication aligns them with the dfs.replication of the cluster.
*/}}
{{- define "fink.sparkconf" -}}
{{ include "fink.s3config" . }}
{{- if and (eq .Values.storage "hdfs") .Values.hdfs.replication }}
spark.hadoop.dfs.replication: "{{ .Values.hdfs.replication }}"
{{- end }}
{{- end }}

{{/* Generate hdfs configuration */}}
{{- define "fink.hdfsconfig" -}}
{{ if eq .Values.storage "hdfs" -}}
- name: SPARK_USER
  value: "{{ .Values.hdfs.hadoop_user_name }}"
{{- end }}
{{- end }}

{{/* Generate common configuration */}}
{{- define "fink.common" -}}
type: Python
pythonVersion: "3"
mode: cluster
image: "{{ .Values.image.repository }}/{{ .Values.image.name }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
imagePullPolicy: "{{ .Values.image.pullPolicy}}"
sparkVersion: "3.5.9"
restartPolicy:
  type: OnFailure
  onFailureRetries: 3
  onFailureRetryInterval: 10
  onSubmissionFailureRetries: 5
  onSubmissionFailureRetryInterval: 20
{{- end }}

{{/*
Night of the jobs: the parameter of the WorkflowTemplate running the night, an
Argo expression resolved at submission.
*/}}
{{- define "fink.nightargs" -}}
- '-night'
- '{{ "{{workflow.parameters.night}}" }}'
{{- /* Same closing rule as the guard: open until then, closed after */}}
- '-night_close_hour'
- '{{ .Values.nightCloseHour }}'
{{- end }}

{{/* Generate common argument for fink-broker command line */}}
{{- define "fink.commonargs" -}}
- '-log_level'
- '{{ .Values.log_level }}'
- '-online_data_prefix'
{{- if .Values.online_data_prefix }}
- '{{ .Values.online_data_prefix }}'
{{- else }}
- 's3a://{{ tpl .Values.s3.bucket . }}'
{{- end }}
- '-producer'
- '{{ .Values.producer }}'
- '-tinterval'
- '{{ .Values.fink_trigger_update }}'
{{- /* Absolute deadline computed once by the guard step, so a restarted job
aims at the same instant */}}
- '-exit_at'
- '{{ "{{inputs.parameters.deadline}}" }}'
{{- if hasSuffix "-noscience" .Values.image.name }}
- '--noscience'
{{- end -}}
{{- end }}

{{/*
Path part of online_data_prefix, e.g.
"hdfs://namenode.hdfs:8020///user/185" -> "/user/185". The report takes the
NameNode address separately (fink.reportNamenode).
*/}}
{{- define "fink.hdfsPath" -}}
{{- $path := regexReplaceAll "^[a-zA-Z0-9]+://[^/]+" .Values.online_data_prefix "" -}}
{{- regexReplaceAll "/{2,}" $path "/" | trimSuffix "/" -}}
{{- end }}

{{/*
NameNode RPC address(es) the report reads HDFS from: report.namenode when
set, host:port[,host:port] with every namenode of an HA pair, else the
authority of online_data_prefix, e.g.
"hdfs://namenode.hdfs:8020///user/185" -> "namenode.hdfs:8020".
*/}}
{{- define "fink.reportNamenode" -}}
{{- if .Values.report.namenode -}}
{{- .Values.report.namenode -}}
{{- else -}}
{{- regexReplaceAll "^[a-zA-Z0-9]+://([^/]+).*$" .Values.online_data_prefix "${1}" -}}
{{- end -}}
{{- end }}

{{/*
Time of day at which a nightly run starts, deduced from nightly.schedule
and formatted as HH:MM. The report uses it to bracket the window during which
a run produced its messages.
*/}}
{{- define "fink.runTime" -}}
{{- $fields := splitList " " .Values.nightly.schedule -}}
{{- printf "%02d:%02d" (atoi (index $fields 1)) (atoi (index $fields 0)) -}}
{{- end }}
