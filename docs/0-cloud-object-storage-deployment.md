# TrainCache 云对象存储部署指南

## 前置：minio-go SDK 是什么

`core/objectstore/s3.go` 引入 `github.com/minio/minio-go/v7`。它是 MinIO 公司发布的 Go S3 客户端库，TrainCache 用它访问任意 S3 兼容后端，不依赖 MinIO Server 存在。

MinIO 公司开源三样东西，在 TrainCache 里各用在一处：

| 项目 | 是什么 | 本项目用法 |
|---|---|---|
| MinIO Server | 对象存储服务，Go 写的 | 本地开发跑在 kind Pod，端口 9000 |
| minio-go SDK | Go 的 S3 客户端库 | `core/objectstore/s3.go` 引入 |
| mc | 命令行 S3 客户端 | 验证连通性与条件写 |

minio-go 把 S3 的 REST 接口封装成 Go 方法，负责四件事：

- AWS V4 签名：按时间戳和 SecretKey 算出签名头。
- URL 风格选择：本地 MinIO 用 `endpoint/bucket/key`，AWS 用 `bucket.endpoint/key`。
- 错误码映射：把服务端返回的 XML 错误码转成 Go error。
- 条件写：`SetMatchETag()` 发 `If-Match`，`SetMatchETagExcept()` 发 `If-None-Match`。

SDK 只发带签名的标准 S3 请求，endpoint 换成任何 S3 兼容后端行为一致：

```go
client, err := minio.New(endpoint, &minio.Options{
    Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
    Secure: secure, // true 走 HTTPS，false 走 HTTP
})
```

选 minio-go 相对 AWS 官方 SDK 的两个依据：模块小、依赖少，对非 AWS 的 S3 实现兼容覆盖最全；条件写有一级封装，CAS 提交和孤儿清理要用。

## 1. 六个环境变量

Agent 启动时读这六个值，解析在 `agent/svc/config.go` 的 `Load()`。都有默认值，缺省连本地 MinIO：`127.0.0.1:9000`、匿名、单桶 `traincache`、HTTP。

| 变量 | 含义 | 默认 |
|---|---|---|
| `TRAINCACHE_OBJSTORE_ENDPOINT` | 对象存储地址。`host:port` 或域名，不带 `http(s)://` | `127.0.0.1:9000` |
| `TRAINCACHE_OBJSTORE_ACCESS_KEY` | AccessKey。腾讯云叫 SecretId，阿里云叫 AccessKeyId | 空（匿名） |
| `TRAINCACHE_OBJSTORE_SECRET_KEY` | SecretKey | 空 |
| `TRAINCACHE_OBJSTORE_BUCKET` | 桶名。桶要先建好 | `traincache` |
| `TRAINCACHE_OBJSTORE_SECURE` | `1`/`true`/`yes` 走 HTTPS，其余走 HTTP | false |
| `TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE` | 启动时探测后端是否支持条件写 | false |

- 协议与地址分开：endpoint 只写域名或 `host:port`，加密与否由 `SECURE` 控制。
- 凭证留空即匿名访问。Agent 只用六个普通客户端操作：Get、Put、Delete、GetETag、PutIfMatch、List，本地桶开匿名读就能跑。
- v1 单桶：所有数据放一个桶，靠对象键前缀隔离 dataset（口径见 6.2 D26）。键布局：

```text
dataset/<dataset>/CURRENT                            ← 当前快照指针
dataset/<dataset>/snapshots/<id>/manifest.json       ← 索引 manifest
dataset/<dataset>/snapshots/<id>/shard-<fp8>.idx     ← 索引分片
dataset/<dataset>/readers/<nodeid>.json              ← 读者心跳声明
<dataset>/blocks/<path>#<chunk>                      ← 数据块，裸数据，不在 dataset/ 下
```

四个键函数在 `core/index/format.go`（`CurrentKey`、`SnapshotPrefix`、`ManifestKey`、`ShardKey`）与 `core/index/readers.go`（`ReaderKey`），块键在 `core/objectstore/blockkey.go`（`BlockKey`）。完整树形口径见 5.0 §对象存储层。

桶根另有 `_traincache/format.json`，内容是 `{"version":1,"chunkSize":N}`，记录整个桶的块大小。发布器首次写数据时创建，之后只读；改块大小必须清空桶重灌或换桶。

- 条件写探测：打开开关后，Agent 启动时写一个临时 key，再用不匹配的 ETag 调 `PutIfMatch`，期望返回 412。写成功说明后端不支持条件写，Agent 告警但不阻止启动。条件写是 CAS 提交和孤儿清理的依赖，后端不支持时走回退路径：乐观写 + 读回校验 + 冲突重试。探测结果上报指标 `traincache_objectstore_conditional_write_supported`，取值 0 或 1。上公有云建议开。

云厂商生产环境必须开 HTTPS，否则请求会被云侧 WAF 拒绝。

## 1.1 可选的合并回源网关路径

Agent 的块 GET 可以经集群内的合并回源网关，其余 S3 方法仍直连源站。网关地址与源站地址分开配置：

| 变量 | 含义 | 默认 |
|---|---|---|
| `TRAINCACHE_GATEWAY_ENDPOINT` | 网关地址，`host:port`，不带 scheme；空表示全部直连源站 | 空 |
| `TRAINCACHE_GATEWAY_SECURE` | `1`/`true`/`yes` 时 Agent 用 HTTPS 连网关，其余走 HTTP | false |

- 只有块 GET 走网关；`Put`、`Delete`、`GetETag`、`PutIfMatch`、`List` 直连源站。不配 `TRAINCACHE_GATEWAY_ENDPOINT` 时块 GET 也直连源站。
- `TRAINCACHE_OBJSTORE_ENDPOINT` / `TRAINCACHE_OBJSTORE_SECURE` 始终指向源站，网关与源站的地址和加密开关各自独立，可以一侧 HTTPS 一侧明文。
- 网关与 Agent 共用 Secret `traincache-objstore` 的 `accessKey`、`secretKey`。

## 2. 三大云厂商的 endpoint 与凭证

| 厂商 | endpoint 格式 | 示例 |
|---|---|---|
| 腾讯云 COS | `cos.<region>.myqcloud.com` | `cos.ap-guangzhou.myqcloud.com` |
| 阿里云 OSS | `oss-<region>.aliyuncs.com` | `oss-cn-hangzhou.aliyuncs.com` |
| AWS S3 | `s3.<region>.amazonaws.com` | `s3.ap-northeast-1.amazonaws.com` |

region 选离训练集群最近的，跨区读会吃满网络延迟。

凭证：在云控制台建一个专用子用户，生成 AccessKey/SecretKey 二元组，权限只授目标桶（见第 6 节）。腾讯云与阿里云的字段名对应关系见上面 env 表。

桶要先在控制台建好，再把桶名填进 `TRAINCACHE_OBJSTORE_BUCKET`。

## 3. 凭证配置

密钥经 K8s Secret 传递，不写明文进 Pod spec：字面值对所有能 `kubectl describe pod` 的人可见，YAML 进了 Git 历史就删不掉。

```bash
# 命令行直接建，避免密钥落到文件里
kubectl create secret generic traincache-objstore-credentials \
  --namespace traincache \
  --from-literal=access-key='AKIDxxxxxxxx' \
  --from-literal=secret-key='xxxxxxxx'
```

Pod spec 里用 `secretKeyRef` 引用：

```yaml
env:
- name: TRAINCACHE_OBJSTORE_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: traincache-objstore-credentials
      key: access-key
- name: TRAINCACHE_OBJSTORE_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: traincache-objstore-credentials
      key: secret-key
```

Secret 内容是 base64 编码，能防误读，防不了有心访问。

本地开发直接在 shell 里 export，密钥不落盘即可：

```bash
export TRAINCACHE_OBJSTORE_ENDPOINT="cos.ap-guangzhou.myqcloud.com"
export TRAINCACHE_OBJSTORE_ACCESS_KEY="AKIDxxxxxxxx"
export TRAINCACHE_OBJSTORE_SECRET_KEY="xxxxxxxx"
export TRAINCACHE_OBJSTORE_BUCKET="my-train-data"
export TRAINCACHE_OBJSTORE_SECURE="1"
export TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE="yes"

go run agent/cmd/traincache-agentd/main.go
```

## 4. 最小部署示例

Agent 以 DaemonSet 部署，每个节点一个。下面是自建 DaemonSet 的最小可用配置。仓库自带的 `controller/deploy/agent/daemonset.yaml` 不写这些字面量：端点、桶、TLS 开关取自 ConfigMap `traincache-config` 的 `objstoreEndpoint` / `objstoreBucket` / `objstoreSecure`，接云端时改那三个配置项即可（[参数配置](PARAMETER-CONFIGURATION.md) §2.1、§2.2）。

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: traincache-agentd
  namespace: traincache
spec:
  selector:
    matchLabels:
      app: traincache-agentd
  template:
    metadata:
      labels:
        app: traincache-agentd
    spec:
      serviceAccountName: traincache-agent
      containers:
      - name: agentd
        image: traincache/agentd:latest
        imagePullPolicy: IfNotPresent
        env:
        - name: NODE_NAME                     # Agent 用它标识自己所在节点
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        - name: TRAINCACHE_OBJSTORE_ENDPOINT
          value: "cos.ap-guangzhou.myqcloud.com"
        - name: TRAINCACHE_OBJSTORE_BUCKET
          value: "my-train-data"
        - name: TRAINCACHE_OBJSTORE_SECURE
          value: "1"
        - name: TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE
          value: "yes"
        - name: TRAINCACHE_OBJSTORE_ACCESS_KEY  # 凭证来自 Secret
          valueFrom:
            secretKeyRef:
              name: traincache-objstore-credentials
              key: access-key
        - name: TRAINCACHE_OBJSTORE_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: traincache-objstore-credentials
              key: secret-key
        ports:
        - name: grpc                          # 监听地址由 TRAINCACHE_GRPC_ADDR 决定，默认 :9410
          containerPort: 9410
          hostPort: 9410                      # Controller 按 nodeIP:9410 拨号下发预热
        - name: metrics                       # 监听地址由 TRAINCACHE_METRICS_ADDR 决定，默认 :9411
          containerPort: 9411
          hostPort: 9411                      # hostPort 让节点上能 curl localhost:9411
        volumeMounts:
        - name: cache-root                    # 预热数据落本地盘，训练 Pod 只读挂载同一路径
          mountPath: /var/cache/traincache
        - name: run                           # agent.sock：训练侧 DataLoader 钩子→Agent，单向 datagram
          mountPath: /run/traincache          # orders/<pod-uid>.ring：训练 Pod 的 sidecar 写，Agent 只读
      volumes:
      - name: cache-root
        hostPath:
          path: /var/cache/traincache
          type: DirectoryOrCreate
      - name: run
        hostPath:
          path: /run/traincache
          type: DirectoryOrCreate
```

## 5. 验证

第一步看日志。endpoint 不通或凭证错，Agent 启动就报错退出。正常输出：

```text
level=info msg="objectstore connected" endpoint=cos.ap-guangzhou.myqcloud.com bucket=my-train-data secure=true
level=info msg="conditional write probe result" supported=yes
```

第二步查条件写指标。COS、OSS、S3 三家都完整支持条件写，期望值为 1：

```bash
curl -s localhost:9411/metrics | grep conditional_write_supported
# 期望：traincache_objectstore_conditional_write_supported 1
```

指标为 0 说明 endpoint 前面有代理层不转发 `If-Match`，CAS 提交走回退路径。

第三步可选，用 mc 手测，三家通用：

```bash
mc alias set mycos https://cos.ap-guangzhou.myqcloud.com '<SecretId>' '<SecretKey>'
mc ls mycos/                              # 能列桶说明凭证与网络都通
mc put mycos/my-train-data/test-key.txt "hello world"
# 用错误 ETag 覆盖，期望返回 412 Precondition Failed
mc --etag="00000000000000000000000000000000" put mycos/my-train-data/test-key.txt "should fail"
```

## 6. 最小权限

凭证只授目标桶和必要动作。腾讯云 COS 的策略示例，在 CAM 控制台附加给那个子用户：

```json
{
  "version": "2.0",
  "statement": [
    {
      "effect": "allow",
      "action": [
        "cos:GetObject",
        "cos:PutObject",
        "cos:DeleteObject",
        "cos:ListBucket",
        "cos:GetBucketLocation"
      ],
      "resource": [
        "qcs::cos:ap-guangzhou:uid/你的APPID:prefix//你的桶名/*"
      ]
    }
  ]
}
```

阿里云 OSS、AWS S3 同样写法：动作换成对应的 `oss:*` 或 `s3:*`，Resource 限定到目标桶和桶内对象。
