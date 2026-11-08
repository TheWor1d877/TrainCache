（TheWor1d877）： [A] 网关「带 Range 的块 GET 按区间合并并落盘」这版改动还没走发版流程：改动在 `gateway/internal/server/server.go` 与 `gateway/internal/cache/`，版本号与 git tag 由用户定，发布清单按 `@sha256` 固定镜像，正式跑分用发布清单镜像，不用集群里的 `:dev` 镜像。契约见 `docs/9.1-design.md` 的网关 HTTP 契约一节。

（TheWor1d877）： [B] Agent 的元数据路径直连源站，一跑给源站多出 5% 到 8% 的流量，桶里有 4232 个对象时一次列举响应约 270 KB。判据口径已把这项流量排除在实验之外，但这部分流量本身还在，要不要把列举改成按前缀列举、或把快照与读者登记的刷新频率降下来。调用点见 `agent/svc/backend.go` 的 `List` 与 `core/index/readers.go`。
