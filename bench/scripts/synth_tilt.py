#!/usr/bin/env python3
"""tilt生成器:多 Job 倾斜访问清单(数据复用 主数据集,不复制)。

- hot-shards.txt:Job A 反复读的 10% 热分片清单;
- full-shards.txt:Job B 全量扫描清单(全部 主数据集分片);
- 到步骤 8(缓存欠配实验)时,Job 访问器按这两个清单构造访问模式。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config


def main():
    config.TILT_DIR.mkdir(parents=True, exist_ok=True)
    shards = sorted(p.name for p in config.DATASET_DIR.glob("shard-*.tar"))
    if not shards:
        sys.exit("主数据集数据不存在,先跑 synth_dataset.py")
    n_hot = max(1, round(len(shards) * config.TILT_HOT_RATIO))
    hot = shards[:n_hot]
    (config.TILT_DIR / "hot-shards.txt").write_text("\n".join(hot) + "\n")
    (config.TILT_DIR / "full-shards.txt").write_text("\n".join(shards) + "\n")
    print(f"tilt 清单:热分片 {n_hot}/{len(shards)} 个(hot-shards.txt),"
          f"全量 {len(shards)} 个(full-shards.txt)")


if __name__ == "__main__":
    main()
