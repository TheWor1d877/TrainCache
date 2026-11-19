{{/*
镜像引用：digest 非空时用 @sha256，否则用 tag。
调用：include "traincache.image" (dict "image" .Values.images.webhook)
*/}}
{{- define "traincache.image" -}}
{{- $image := .image -}}
{{- if $image.digest -}}
{{- printf "%s@%s" $image.repository $image.digest -}}
{{- else -}}
{{- printf "%s:%s" $image.repository $image.tag -}}
{{- end -}}
{{- end -}}