（TheWor1d877）： [A] 网关「带 Range 的块 GET 按区间合并并落盘」这版改动还没走发版流程：改动在 `gateway/internal/server/server.go` 与 `gateway/internal/cache/`，版本号与 git tag 由用户定，发布清单按 `@sha256` 固定镜像，正式跑分用发布清单镜像，不用集群里的 `:dev` 镜像。契约见 `docs/9.1-design.md` 的网关 HTTP 契约一节。

（TheWor1d877）： [A] 网关族 G2 臂 2026-11-08 实测的源站出站是 4.65 GB，比一份 4 GiB 工作集多 0.36 GB，这部分流量没有经过网关（片段缓存已经覆盖整个工作集，来源站回源的片段与整块回源的字节 sha256 相同）。要查清这 0.36 GB 是谁拉的，再定判据「G2 对 G0 的源站字节比落在 12 ± 5%」怎么判。口径见 `docs/4.2-bench-families.md` 的网关族一节。

（TheWor1d877）： [A] 网关的 60 秒汇总日志行会被日志采样器的 60 秒去重窗口丢掉：汇总行的去重键是 (component, message, node, task_id, reason)，每行这五项都一样，落进同一个 60 秒窗口的行被丢弃。2026-11-08 的 G2 臂实测 8 个窗口里有 3 个没落进日志，`summary.csv` 的 `gateway_gets`、`gateway_merged`、`gateway_bytes` 等列因此是下界。要让每行有独立去重键，或给汇总行加不重置的累计计数，再重跑网关族。
