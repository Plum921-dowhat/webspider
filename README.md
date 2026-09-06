# WebSpider（en-tech-pipeline）

英文技术文章语料采集-清洗-入库-导出流水线：从多个技术内容源抓取文章，清洗为 Markdown 语料，存入 PostgreSQL，最终导出 `corpus.jsonl` 供 LLM 微调（SFT）或 RAG 向量库使用。

## 架构

```
DEV.to API ──┐
RSS feeds ───┤   每个 source 一个 crawler 进程（同镜像、独立配置）
SE API ──────┘   共用一条 Redis Stream「articles」
   │
   ▼
Redis Stream ←── 生产端 Lua 脚本原子去重（SISMEMBER + SADD + XADD）
   │  消费组「py」
   ▼
Python 处理器（py-processor/main.py）
   ├─ 正文直出快速路径（payload 带 content_md 的消息跳过抓取/提取）
   ├─ 并发抓取 HTML（全局 + 每域名双层令牌桶，429 全池惩罚）
   ├─ trafilatura 提取 Markdown（保留 ``` 代码块）
   ├─ 质量门控（宽进严出：spam → code_rich 豁免 → link_dump → 词汇多样性 …）
   ├─ 语言过滤（langdetect，在去代码后的文本上检测）
   ├─ 近重复去重（SimHash 64-bit + LSH 分带，状态持久化在 Redis）
   └─ PostgreSQL 存储（url_hash 唯一，ON CONFLICT DO NOTHING）
   ▼
FastAPI 仪表盘（http://localhost:8000，只读，按语言/来源聚合）
   ▼
export.py → corpus.jsonl（自包含语料导出）
```

## 数据源

| 源 | 配置文件 | 说明 |
| --- | --- | --- |
| `devto` | `config*.yaml` | DEV.to `/api/articles/latest`（严格最新优先），HTML 抓取路径 |
| `rss` | `config.rss*.yaml` | 通用 RSS/Atom 适配器，`rss_feeds` 列表驱动；单页轮询，7 天窗口（博客低频），去重 SET 保证重复轮询幂等 |
| `stackexchange` | `config.stackexchange*.yaml` | Stack Overflow 问答对：`sort=activity` 抓"最近被回答"的题目，`se_require_answered` 只收有采纳/高票答案的题目，Go 端组装为 `# 标题 + 问题 + ## Top answer` 的 Markdown **正文直出**（处理器零抓取）。匿名配额 300 请求/天/IP，`SE_API_KEY` 环境变量（免费 key）提升到 10k/天 |

接入新源：实现 `source.Source` 接口（`internal/source/source.go`），在 `cmd/crawler/main.go` 的 `sourceRegistry` 注册，加一份 `config.<source>*.yaml`，compose 加一个 `crawler-<source>` 服务（`command:` 只传配置路径）。正文可直接从 API 拿到的源（如 Stack Exchange）把 Markdown 放进 `ArticleRaw.ContentMD`，处理器自动走快速路径。

每个源一个独立进程（同镜像、不同配置），速率/窗口/间隔按源独立调优。**注意**：Hashnode GraphQL API 自 2026-05 起需要 Pro 订阅，未接入。

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `go-crawler/` | Go 生产者：`cmd/crawler`（入口）、`internal/source`（数据源抽象 + DevTo 实现）、`internal/scheduler`（worker pool + 限速 + 熔断）、`internal/stream`（Redis Stream 生产者） |
| `py-processor/` | Python 消费者：`main.py`（主管道）、`fetcher.py`（限速抓取）、`quality.py`（质量门控）、`dedupe.py`/`neardup.py`（SimHash LSH 去重）、`store.py`（PG 连接池 + upsert）、`export.py`、`dlq_replay.py`（死信重放）、`dashboard/`（看板） |
| `db/` | `init.sql`（建表 + 索引）、`migrations/`（存量库迁移） |
| `docker-compose.yml` | redis / postgres / crawler / processor / dashboard 五服务编排 |

## RAG 出口

采集入库只是上半场，`py-processor` 内置了到向量检索的三步管道：

```bash
# 1) 分块导出（增量：state 文件记录上次导出的文章 id，重复执行只出新数据）
docker compose exec processor python rag_export.py            # 首次建议 --full
docker compose cp processor:/tmp/corpus_chunks.jsonl corpus_chunks.jsonl

# 2) 生成 embedding（OpenAI 兼容接口，key 填在 .env，见 .env.example）
docker compose exec processor python embed_chunks.py \
    --chunks /tmp/corpus_chunks.jsonl --out /tmp/corpus_vectors.npz
docker compose cp processor:/tmp/corpus_vectors.npz corpus_vectors.npz

# 3) 导入 pgvector（需把 compose 中 postgres 镜像换成 pgvector/pgvector:pg16）
docker compose exec processor python import_pgvector.py \
    --chunks /tmp/corpus_chunks.jsonl --vectors /tmp/corpus_vectors.npz
```

每个 chunk 携带完整溯源元数据：`chunk_id / url / title / heading_path（如 ["文章标题", "Top answer"]）/ chunk_index / n_chunks / tags / source_type / published_at / quality_score`。分块策略按 Markdown 标题层级切分，超长段在段落边界二次切分并带 150 字符重叠，代码块永不硬切。

## 监测

管道自带两层监测：

- **看板「采集监测」面板**（实时）：每个源的运行心跳（最近一轮时间/页数/投递/失败页 + 绿/红新鲜度灯，阈值由 `STALE_THRESHOLDS` 环境变量控制，默认 devto 120 分钟、SE 300 分钟、RSS 180 分钟），以及 24h 分源活动速率图。心跳由 Go 调度器每轮写入 Redis `pipeline:runs`；`monitor` 采样服务每 60 秒把全量指标快照进 Postgres `pipeline_snapshots`（保留 7 天），看板宕机也不丢历史。
- **`/api/health` 端点**：上述数据的机器可读版本，可接外部告警（如 cron 脚本、Uptime Kuma）。

机器休眠/容器停止期间不会有心跳，面板会如实显示源停滞——这是预期行为，不是故障。

## 快速开始

```bash
docker compose up -d --build
# 看板: http://localhost:8000

# 导出语料（可按分数/语言/长度过滤）
docker compose exec processor python export.py --out /tmp/corpus.jsonl --min-len 200 --min-score 0.3 --lang en
docker compose cp processor:/tmp/corpus.jsonl corpus.jsonl

# 重放死信（extract_fail 风暴过后）
docker compose exec processor python dlq_replay.py --dry-run
docker compose exec processor python dlq_replay.py --delete

# 质量阈值标定（dry-run，扫库输出拒绝原因直方图）
docker compose exec processor python quality_probe.py --limit 5000
```

本地开发：

```bash
# Go
cd go-crawler && go test ./... && go run ./cmd/crawler config.yaml

# Python（需本地 redis/postgres，或 python 3.11+）
cd py-processor
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt -r requirements-dev.txt
python -m pytest tests -q
```

## 配置

Go 端读 YAML（`config.yaml` 本地 / `config.docker.yaml` 容器），Python 端全部环境变量（见 `py-processor/config.py`，均有默认值）。关键项：

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `crawler.since_hours` | 24 | 增量窗口：只抓最近 N 小时发布的文章 |
| `crawler.interval_minutes` | 60 | >0 守护模式循环抓取；0 跑一次退出 |
| `crawler.rate_per_sec` | 3 | 对 DEV.to API 的请求速率 |
| `crawler.max_page_failures` | 3 | 熔断：单次运行连续失败 N 页即中止（防上游故障风暴） |
| `redis.stream_max_len` | 100000 | 流上限（XADD MAXLEN ~ N），0 不限 |
| `FETCH_QPS` / `FETCH_WORKERS` | 6 / 8 | 处理端抓取 HTML 的全局限速与并发 |
| `SIMHASH_HAMMING` | 3 | 汉明距离 ≤ 阈值判为近重复 |
| `SIMHASH_BANDS` | 4 | LSH 分带数（必须整除 64） |
| `MIN_CODE_CHARS` | 80 | code_rich 豁免门槛：代码字符达标直接保留 |
| `LANG_KEEP` | en | 保留的语言 |

## 可靠性设计

- **原子去重**：URL SHA-256 经 Lua 脚本「查重→记录→XADD」一次完成，重复 URL 永不进入流。
- **不丢数据**：整批只有在全部成功后才 `XACK`。抓取/提取失败进死信流 `articles_dlq`（幂等，MAXLEN 截断）；DB 故障时消息留在 PENDING，重连后 `recover_pending()` 回收重放。
- **先写库再建指纹**：SimHash 指纹在 INSERT 成功后才写入 LSH，重试不会被自己的指纹拒绝。
- **增量安全重跑**：PG `ON CONFLICT (url_hash) DO NOTHING` + 处理端 SimHash 双保险，重复消息幂等。

## 去重参数的取舍

SimHash LSH：64 位指纹分 `SIMHASH_BANDS` 带。**汉明距离 ≤ `SIMHASH_HAMMING`(3) 的文档必然被捕获**——4 带之下 ≤3 个翻转位至多污染 3 条带，鸽笼原理保证至少一条带完全一致。概率性体现在反方向：略超阈值（距离 4+）的文档只被偶然捕获；不相关文档也可能共享某条带（随后被精确汉明校验过滤）。阈值 3 对 ~100 词的短文偏紧（改 2 个词约移动 2–4 位），对千字长文很宽松；如需更宽容的短文去重，调大 `SIMHASH_HAMMING` 或调小 `SIMHASH_BANDS`（=2 时 32 位/带）。

## 测试与 CI

- Go：`cd go-crawler && go test ./...`（source 解析、stream 哈希等纯函数）。
- Python：`python -m pytest tests -q`（质量门控、SimHash/LSH、限速器；LSH 用内存桩，无需真实 Redis）。
- GitHub Actions（`.github/workflows/ci.yml`）：go build/vet/test + pytest。

## 存量库升级

首次 `docker compose up` 自动应用 `db/init.sql`。已运行的库需手动应用迁移（如 trigram 搜索索引）：

```bash
cat db/migrations/001_add_trgm.sql | docker compose exec -T postgres psql -U crawler -d corpus
```
