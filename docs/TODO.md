（TheWor1d877）： [A] 网关族 G2 臂的判据口径待定：「G2 对 G0 的源站字节比落在 12 ± 5%」这个比值现在按 MinIO 的总出站算，会把 Agent 元数据路径的直连流量算进 G2 那一侧。Agent 的 List、GetETag、Put 按设计直连源站（`agent/svc/backend.go`），快照与读者登记刷新会反复列举桶里的对象，一跑 340 次、每次响应约 270 KB，这部分占源站出站的 5% 到 8%；G0 臂没有 agentd，不产生这项流量，比值因此偏低。要定的是 G2 臂取哪个数：网关记的源站字节（三跑 4.26 GB，对 G0 的 50.89 GB 是 11.95，落在带内）还是 MinIO 总出站减掉元数据流量（三跑 4.65、4.61、4.49 GB，比值 10.93、11.05、11.32，都在带外）。实测见 `docs/4.2-bench-families.md` 的网关族一节。

（TheWor1d877）： [A] 网关「带 Range 的块 GET 按区间合并并落盘」这版改动还没走发版流程：改动在 `gateway/internal/server/server.go` 与 `gateway/internal/cache/`，版本号与 git tag 由用户定，发布清单按 `@sha256` 固定镜像，正式跑分用发布清单镜像，不用集群里的 `:dev` 镜像。契约见 `docs/9.1-design.md` 的网关 HTTP 契约一节。

（TheWor1d877）： [B] Agent 的元数据路径直连源站，一跑给源站多出 5% 到 8% 的流量，桶里有 4232 个对象时一次列举响应约 270 KB。要不要把列举改成按前缀列举、或把快照与读者登记的刷新频率降下来，让源站出站口径只反映训练数据的流量。调用点见 `agent/svc/backend.go` 的 `List` 与 `core/index/readers.go`。
