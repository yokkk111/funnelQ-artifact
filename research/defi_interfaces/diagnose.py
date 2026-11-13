import json
import zlib
import numpy as np
import run as exp


def main():
    a=np.load(exp.HERE/'events.npy');n=len(a)
    parts=[np.flatnonzero(a['kind']==0),np.flatnonzero((a['kind']==2)&(a['sign']>0)),np.flatnonzero((a['kind']==2)&(a['sign']<0)),np.flatnonzero(a['kind']==1),np.flatnonzero(a['kind']==1)]
    ids=np.concatenate(parts).astype(np.uint32)
    own=np.concatenate([np.full(len(x),i,dtype=np.uint8) for i,x in enumerate(parts)])
    actor=a['actor'][ids]
    asset=np.concatenate([a['asset'][x] if i!=4 else a['out'][x] for i,x in enumerate(parts)])
    masks={}
    for mode,keys in [('actor_only',actor.astype(np.uint64)),('actor_asset',(actor.astype(np.uint64)<<32)|asset)]:
        order=np.argsort(keys,kind='stable');v=keys[order]
        st=np.r_[0,1+np.flatnonzero(v[1:]!=v[:-1])]
        bits=np.bitwise_or.reduceat(np.left_shift(np.uint8(1),own[order]),st)
        m=np.empty(len(ids),dtype=np.uint8)
        m[order]=np.repeat(bits,np.diff(np.r_[st,len(v)]));masks[mode]=m
    results=json.loads((exp.HERE/'results.json').read_text())
    rows=[]
    for (label,w,_,q),r in zip(exp.prior.workloads(),results):
        assert label==r['label'] and w==r['window']
        _,req=exp.compile_query(q)
        bases={atom['id']:exp.atom_mask(a,q,atom) for atom in q['events']}
        base_union=np.logical_or.reduce(list(bases.values()))
        controls={}
        for mode,m in masks.items():
            union=np.zeros(n,dtype=bool)
            for atom in q['events']:
                domain=bases[atom['id']].copy()
                for p,need in req[atom['id']]:
                    admitted=np.zeros(n,dtype=bool)
                    admitted[ids[(own==p)&((m&need)==need)]]=True
                    domain &= admitted
                union |= domain
            controls[mode+'_unbounded_time_event_union']=int(union.sum())
            assert int(union.sum())>=r['selected_event_union']
        answers=np.load(exp.HERE/(f'{label}_w{w}_answers.npz'))['full']
        used=np.unique(answers.view('<i8'));on_answer=np.isin(a['eid'],used)
        row={'label':label,'window':w,'local_baseline_unique_events':int(base_union.sum()),**controls,
             'scoped_interface_event_union':r['selected_event_union'],
             'reduction_vs_local_baseline_percent':100*(1-r['selected_event_union']/base_union.sum()),
             'baseline_event_kernel_zlib1_bytes':len(zlib.compress(a[base_union].tobytes(),1)),
             'oracle_answer_used_event_count':len(used),
             'candidate_to_answer_event_ratio':r['selected_event_union']/max(1,len(used)),
             'oracle_answer_used_kernel_zlib1_bytes_NOT_a_valid_certificate':len(zlib.compress(a[on_answer].tobytes(),1)),
             'extra_domain_events_not_in_answers':r['selected_event_union']-len(used)}
        rows.append(row)
        print(label,w,'base',row['local_baseline_unique_events'],'actor',controls['actor_only_unbounded_time_event_union'],'joint',controls['actor_asset_unbounded_time_event_union'],'scope',r['selected_event_union'],'answer_events',len(used),flush=True)
    exp.save('diagnostics.json',rows)
    offline=json.loads((exp.HERE/'offline.json').read_text())
    source=json.loads((exp.HERE/'source.json').read_text())
    summary=json.loads((exp.HERE/'run_summary.json').read_text())
    lines=['DeFi 联合接口能力域：三个月完整数据结构实验',
           '结论：Conditional Go。20/20 配置完整答案集合一致；19/20 结构载荷估计低于 1.74 MiB，S3 为 10.46 MiB。',
           '重要：没有实现密码学认证。载荷包含目录及模拟32字节成员根、完整成员ID、事件kernel、答案；缺少真实事件认证路径/其他协议开销。不是最终VO。',
           'SQL测试时间不是 Rust 计数内核、不是客户端验证时间、不是端到端认证延迟。',
           f"数据：{source['events']:,} 事件，{source['transactions']:,} 交易；只读原始基础表，未使用Cand2/hit mask。",
           '工作负载：S1-S5 SameTx；E1-E5 分别60/300/600秒，全部20配置。',
           '正确性：DuckDB在完整事件原子域和缩减域上各自枚举全部binding，逐集合差比较，FP/FN均0；与历史正式全量结果计数也全部一致。',
           '金额：只对源库有限REAL的正负进行无损分类；6条零金额Borrow留在源数据但不进正/负端口，所有本轮Borrow原子都明确要求正或负。',
           '构建不读取查询。查询编译器按共享变量产生端口需求，没有按Q编号分支。SameTx严格按tx分桶；Episode使用8..1024二倍尺度。',
           '候选域允许包含不参与答案的事件；FP/FN=0指最终完整查询答案，而不是每条候选都有效。',
           f"离线：源载入及缓存 {source['load_seconds_including_save']:.3f}s；额外结构构建与写盘 {offline['offline_seconds_excluding_source_load']:.3f}s。单次本机观察，不是稳态多次benchmark。",
           f"增量索引 {offline['additional_index_bytes']/2**20:.3f} MiB；共享事件缓存 {source['event_cache_bytes']/2**20:.3f} MiB 单列，不算免费。",
           f"离线进程峰值 {offline['peak_process_rss_bytes']/2**20:.1f} MiB；全实验峰值 {summary['peak_rss_bytes']/2**20:.1f} MiB（含SQL对照、缓存，不是客户端峰值）。",
           f"端口incidence {offline['port_incidence']:,}；SameTx能力类 {offline['same_tx']['profiles']}；八尺度联合first-capability profiles {offline['episode']['profiles']}。",
           '没有构建事件对support graph，完整成员表按profile排序，共享事件记录不按尺度复制。',
           '', 'Query | Window | Base unique events | Reduced unique events | Member IDs | Answers | Structural MiB | SQL reduced COUNT ms | SQL reduced replay ms']
    for r,d in zip(results,rows):
        lines.append(f"{r['label']} | {r['window']} | {d['local_baseline_unique_events']} | {r['selected_event_union']} | {r['opened_member_ids_dedup_classes']} | {r['answer_count']} | {r['structural_compressed_response_excluding_merkle_frontier']/2**20:.3f} | {r['reduced_sql_count_s']*1000:.2f} | {r['reduced_sql_replay_s']*1000:.2f}")
    lines += ['', 'S3瓶颈：',
              '725,039个最终事件；但求交前必须完整打开3,294,174个成员ID。成员ID压缩4.425MiB，事件kernel压缩5.777MiB，答案约0.257MiB。',
              'Swap的两个端口分别认证、再按eventID求交，认证成员体积显著大于最终事件域；这是结构性成本，不能靠少发未认证成员规避。',
              'S3只有110,345个真实答案使用的事件。端口共现未表达完整资产回路、先后顺序、池不等，所以保留了大量无用候选；前10 actor占保留域64.7%，但不据此臆断其都是router。',
              'E1也有明显松弛：60秒保留83,627事件，真正参与答案999事件；接口摘要未表达跨池不等与精确顺序。',
              '', '下一步：',
              '1. 不宣布所有查询轻量；先解决多端口联合成员展开及必要域松弛。',
              '2. 可先研究schema级联合端口能力分类，直接认证同一event的接口组合，而不是分别传大列表后求交；不得添加S3专门分支。',
              '3. 联合分类减少ID传输不等于缩小事件域；顺序、不同池等条件仍需另行证明安全的通用结构，不能偷偷执行未认证剪枝。',
              '4. 结构通过后才接真实完整类别/事件认证，再测最终VO、服务端生成和客户端count+sound+unique；本次没有这些指标。',
              '', '复现：python research/defi_interfaces/run.py；python research/defi_interfaces/diagnose.py',
              '结果：results.json逐配置原始指标；diagnostics.json结构对照；offline.json/source.json/run_summary.json；每配置answers.npz保存完整域和缩减域全部binding。']
    (exp.HERE/'report.txt').write_text('\n'.join(lines),encoding='utf-8')


if __name__=='__main__':main()
