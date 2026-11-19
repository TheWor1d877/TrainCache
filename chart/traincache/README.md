# TrainCache Helm chart

把 `controller/deploy` 的 kustomize 渲染结果按 values 参数化。chart 不改变镜像内容，
`appVersion` 指向当前发布版本；chart 自身用独立的 `version`。

## 安装

前置条件：Kubernetes 1.28+ 集群、Helm 3、一个 S3 兼容桶。

**1. 建 webhook 服务端证书。** Secret 名默认 `traincache-webhook-tls`，键为
`tls.crt` 与 `tls.key`，与 Release 同 namespace：

```bash
NS=traincache; SVC=traincache-webhook
TLS=$(mktemp -d)
openssl req -x509 -newkey rsa:2048 -nodes -days 365 -keyout "$TLS/ca.key" -out "$TLS/ca.crt" -subj "/CN=traincache-ca"
openssl req -newkey rsa:2048 -nodes -keyout "$TLS/tls.key" -out "$TLS/tls.csr" -subj "/CN=$SVC"
printf 'subjectAltName=DNS:%s,DNS:%s.%s.svc.cluster.local\n' "$SVC" "$SVC" "$NS" >"$TLS/san.ext"
openssl x509 -req -in "$TLS/tls.csr" -CA "$TLS/ca.crt" -CAkey "$TLS/ca.key" -CAcreateserial -out "$TLS/tls.crt" -days 365 -extfile "$TLS/san.ext"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create secret tls traincache-webhook-tls --cert="$TLS/tls.crt" --key="$TLS/tls.key"
```

**2. 建对象存储凭据 Secret。** 默认名 `traincache-objstore`，键为 `accessKey`
与 `secretKey`：

```bash
kubectl -n "$NS" create secret generic traincache-objstore \
  --from-literal=accessKey=<AK> --from-literal=secretKey=<SK>
```

**3. 安装。** 证书的 CA 通过 `webhook.caBundle` 一次填入，不再装完再 patch：

```bash
helm install traincache ./chart/traincache -n traincache \
  --set webhook.caBundle="$(base64 -w0 "$TLS/ca.crt")" \
  --set config.objstoreEndpoint=minio.minio.svc.cluster.local:9000 \
  --set config.objstoreBucket=traincache
```

对象名固定（ServiceAccount、ClusterRole、ConfigMap 等），同一集群只装一个 release。

## 常用 values

| 键 | 说明 |
|---|---|
| `images.<组件>.tag` / `.digest` | 五个部署组件；`digest` 非空时覆盖 `tag` |
| `webhook.checkpointImage` | 注入训练 Pod 的 checkpointd 镜像 |
| `manager.replicas`、`webhook.replicas`、`scheduler.replicas` | 副本数；gateway 固定 1，不提供 values |
| `*.resources` | 各组件的 requests 与 limits |
| `objstore.existingSecret` | S3 凭据 Secret 名 |
| `config` | `traincache-config` 的 22 个配置项 |
| `schedulerConfig` | `traincache-scheduler-config` 的 2 个配置项 |
| `agent.cacheHostPath`、`agent.runHostPath` | Agent 缓存的节点 hostPath 与 ring 目录 |
| `webhook.existingSecret`、`webhook.caBundle` | webhook 服务端证书与签发 CA |

完整默认值用 `helm show values ./chart/traincache` 查看。

## 升级与卸载

```bash
helm upgrade traincache ./chart/traincache -n traincache -f values-prod.yaml
helm uninstall traincache -n traincache
```

`helm upgrade` 不会更新 `crds/` 里的 CRD。升级 CRD 时只 apply CRD 文件：

```bash
kubectl apply -f chart/traincache/crds/
```

`helm uninstall` 不删除 CRD，也不删除 CRD 里的自定义资源。

## 与 kustomize 的关系

`controller/deploy` 仍是 bench 装置与 `install.yaml` 发布渲染的入口，本 chart 与它同源。
`make verify-chart` 比较两边渲染出来的对象，默认 values 下除 Namespace 外应完全一致。

安装后不要再用 `kubectl apply -k` 改同一批对象：ConfigMap 与 Deployment 由 Helm
管理，手工 patch 会在下一次 `helm upgrade` 时被覆盖。改参数用 `helm upgrade --set`
或 `-f`。