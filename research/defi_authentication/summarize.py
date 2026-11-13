import json
import statistics
from pathlib import Path

HERE=Path(__file__).resolve().parent
out=HERE/'adaptive_compact'
rows=json.loads((out/'results.json').read_text())
isolated=json.loads((out/'isolated_clients.json').read_text())
clients={(r['label'],r['window']):r for r in isolated}
offline=json.loads((HERE/'offline_compact.json').read_text())
raw={(r['label'],r['window']):r for r in json.loads((HERE/'results.json').read_text())}
assert len(rows)==len(isolated)==20
assert all(r['fp']==r['fn']==0 for r in rows)
assert all(r['answer_count']==clients[(r['label'],r['window'])]['answer_count'] for r in rows)
lines=[
    'DeFi 联合接口完整分类认证实验（本机 CPU，2026-10-27）',
    '',
    '结论：当前受支持谓词片段/完整三个月 workload 的结构与认证包实验通过；生产接入仍为 Conditional Go。',
    '不是任意 QuerySpec 的全覆盖协议，也未证明计数＋验真比重放更快。',
    '',
    '1. 机制',
    '离线按固定的 F、B+、B-、Swap-input、Swap-output 接口构建能力。',
    '每条事件的全部接口放在同一联合 profile 内；Swap 输入输出不再作为两份独立 posting 传输。',
    'SameTx 使用精确 tx 分区；Episode 使用 8..1024 秒二倍尺度的嵌套保守覆盖。',
    '离线不读取查询。运行时由 QuerySpec 共享变量编译每个原子的必要接口集合；无 S/E 查询编号分支。',
    '客户端认证完整分类目录，并自己计算必须打开的全部分类。每个选中分类必须返回全部事件。',
    '类叶哈希绑定上下文、分类位置、行数、完整事件 kernel；实际 SHA256 frontier 认证未打开子树。',
    '只证明保守候选域完整，不证明每个保留事件都能参与答案。',
    '客户端从认证域独立 SQL 精确计数，并对返回完整 binding 验真、检查唯一性、比较数量。',
    '同一字节差分/原值自适应编码适用于所有查询，不删字段、不改变语义。',
    '',
    '2. 数据及正确性',
    '完整支持事件 3,678,059 条：Flashloan 620,498，Swap 2,721,797，Borrow 335,764。',
    'S1-S5 + E1-E5 × 60/300/600 秒，共 20 配置。各配置与上一轮全源 SQL 完整答案集做 FP/FN 集合比较，均为 0。',
    '上一轮全源 SQL 已与历史正式结果计数对齐；本轮未重新执行全量 Rust Generic Residual。',
    '当前每个原子的保留域并未引入更强过滤：总保留行数与旧接口实验完全相同，最终结果集合完全一致。',
    f'10 类攻击 × 20 配置 = {sum(len(r["attacks"]) for r in rows)} 项全部拒绝。',
    '包括改查询、改 epoch、目录篡改、事件篡改、缺失/乱序分类、以 root frontier 绕过打开叶、截断包、重复答案、伪造 ID、少报答案。',
    '另外 16 组通用编码往返测试，覆盖空数组、单行、随机值及有符号极值溢出。',
    '所有 20 包还由独立客户端进程验证：不加载源事件表、不加载服务器目录/树、不加载 oracle 答案。',
    '',
    '3. 实测主表',
    '单位 MiB=1048576 bytes；响应含目录、事件、实际 frontier、答案及 wire header。可信 Manifest 是预先认可的外部输入，约 0.7 KiB/模式。',
    'E2E = 服务器选域/建 SQL 表/枚举答案 + 认证包生成 + 客户端解码认证/建表/计数/验真去重。',
    '不含网络、磁盘读取、Python import、离线封存或区块链读取；本机单次阶段观测，不是服务器 GPU 正式性能结论。',
    '独立客户端列来自单独进程，不能与同进程分项强行相加。',
    '',
    'Query | Window | Selected events | Answers | Old estimate MiB | New actual MiB | Packet-gen s | Client s | E2E s | Isolated client s',
]
for r in rows:
    c=clients[(r['label'],r['window'])]
    lines.append(f"{r['label']} | {r['window']} | {r['selected_rows']} | {r['answer_count']} | {r['old_structural_estimate_bytes']/2**20:.4f} | {r['wire_bytes']/2**20:.4f} | {r['packet_generation_s']:.4f} | {r['client_total_s']:.4f} | {r['e2e_no_network_s']:.4f} | {c['client_total_s']:.4f}")
lines.extend([
    '',
    '4. 大小分解（bytes，非估算）',
    'Query | Window | Event columns | Complete directory | Frontier (including positions) | Answer payload | Other wire header | Raw-shuffle packet control',
])
for r in rows:
    other=r['wire_bytes']-r['event_column_zlib_bytes']-r['directory_zlib_bytes']-r['frontier_bytes']-r['answer_zlib_bytes']
    lines.append(f"{r['label']} | {r['window']} | {r['event_column_zlib_bytes']} | {r['directory_zlib_bytes']} | {r['frontier_bytes']} | {r['answer_zlib_bytes']} | {other} | {raw[(r['label'],r['window'])]['wire_bytes']}")
lines.extend([
    '',
    '旧值只是结构估算，未包含真实事件认证路径；新值是真实序列化认证包。两者不是旧协议/新协议同环境端到端性能对照。',
    '收益首先来自同事件接口联合分类，去掉求交前的成员 ID；其次是无损列编码。S3 仍保留 725,039 事件，不是安全过滤更强了。',
    '自适应列编码在某些 Episode 小分类上会略增 header/压缩开销，并非所有配置都缩小或变快。',
    '',
    '5. 离线/内存',
    f"新活动索引 {offline['physical_index_bytes']} bytes = {offline['physical_index_bytes']/2**20:.4f} MiB；包含两模式的行排列、完整目录、实际哈希树、Manifest。",
    '共用已有 133.292 MiB 事件 kernel 表，不再重复持久化两套事件内容。',
    '旧结构索引为 48.857 MiB（两组端口 member lists），新索引为每事件每模式一个排列位置。',
    f"从内存映射的完整事件 kernel 重建能力、分类、排序、计算根并写索引：{offline['seconds_rebuild_sort_hash_write']:.4f} 秒。",
    '不含从原始日志生成 canonical events、读取原 SQLite、独立审核者重建/签署与链上开销。',
    f"独立客户端峰值 RSS 范围：{min(c['peak_client_process_rss_bytes'] for c in isolated)/2**20:.2f}–{max(c['peak_client_process_rss_bytes'] for c in isolated)/2**20:.2f} MiB（包含 Python/DuckDB runtime）。",
    '旧实验的大事件文件仍原样保留；磁盘上的所有历史产物之和不等于部署所需活动索引大小。本轮没有删除文件。',
    '',
    '6. 计数与重放：仍需保留负结论',
    '只比较 count 内核，会漏掉计数路径额外的 Sound/Unique 成本。两种路径可共用同一认证域/认证包。',
    'Query | Window | Median warm count s | Sound/Unique s (single trial) | Count+Sound/Unique s | Median warm replay/sort/compare s',
])
for r in rows:
    cnt=statistics.median(x['count'] for x in r['warm_kernel_repeats'])
    replay=statistics.median(x['replay'] for x in r['warm_kernel_repeats'])
    lines.append(f"{r['label']} | {r['window']} | {cnt:.5f} | {r['client_sound_unique_s']:.5f} | {cnt+r['client_sound_unique_s']:.5f} | {replay:.5f}")
lines.extend([
    '当前 Python 路径中，计数＋验真去重通常不优于重放。不能把本轮认证域的传输/证明生成收益归因于计数封印本身。',
    '',
    '7. 安全边界与未完成内容',
    '本实验有真实 Merkle 验证，但可信根由本机独立 builder 生成；并未接入正式 EpochRoot、链上签名或审核者。',
    '根固定的是实验 kernel 与派生 profile 的对应结果。恶意封存者若故意错误分类，单靠 Merkle 无法发现；必须由可信独立审核者从完整源重建。',
    '当前支持的原子为 Flashloan/Swap/正负 Borrow；接口是 actor+asset，窗口不超过 1024 秒；pool 等/不等、严格 order、tx 与 injectivity 在最终 SQL/验真中执行。',
    'NoEvent、任意数值谓词、未知事件类型或超出尺度的窗口尚无通用认证 fallback；该 runner 应拒绝，不能声称任意 QuerySpec 都适用。',
    '没有 S1/S3/Q1 等运行时专用分支；label 仅用于测试文件与报告。相同类型/角色接口规则适用于所有 20 配置。',
    '未修改正式 FunnelQ/CSR/Residual/Manifest/SP1；未删除任何数据。',
    '',
    '8. 下一步',
    '结构及 authenticated-package 原型：Go。正式可信封存集成、协议边界和公平端到端对照：仍 Conditional Go。',
    '应先接入独立审核的源 kernel/root 绑定并加入新查询组合/边界测试，再迁移 Rust 通用验证器并做多次冷/热试验；无需先继续设计百万规模多项式证明。',
    '若论文必须证明计数优于重放，需要单独测 Count+Sound/Unique 与 Replay，或诚实地将主要贡献定位于完整域认证，不能用共同前置收益充当计数优势。',
    '',
    '复现（工作目录为主仓库根；依赖沿用上一轮独立 deps/，无需修改系统 Python）：',
    'python research/defi_authentication/run.py --adaptive',
    'python research/defi_authentication/isolated_clients.py',
    'python research/defi_authentication/summarize.py',
    '若需要重新测离线构建：在第一条命令增加 --rebuild。',
    '活动索引：compact_indexes/；最终数据与实际 packet：adaptive_compact/。',
])
(HERE/'report.txt').write_text('\n'.join(lines)+'\n',encoding='utf-8')
summary={'configurations':20,'fp':0,'fn':0,'max_response_bytes':max(r['wire_bytes'] for r in rows),
         'all_under_5_MiB':all(r['wire_bytes']<=5*2**20 for r in rows),
         'all_under_5_decimal_MB':all(r['wire_bytes']<=5_000_000 for r in rows),
         'e2e_range_s':[min(r['e2e_no_network_s'] for r in rows),max(r['e2e_no_network_s'] for r in rows)],
         'active_index_bytes':offline['physical_index_bytes'],'isolated_clients':len(isolated),
         'decision':'Go for structural/auth-package prototype; Conditional Go for production integration',
         'count_advantage_not_established':True}
(HERE/'final_summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
print(json.dumps(summary,indent=2))
