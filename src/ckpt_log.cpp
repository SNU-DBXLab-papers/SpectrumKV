#include "ckpt_log.h"
#include "pmemManager.h"
#include "pmemInodePool.h"
#include <cstring>
#include <vector>
#include <cassert>
#include <cstddef>
#include <type_traits>

#ifndef CKPLOGPOOL
#define CKPLOGPOOL 3
#endif

using namespace std;

#if ENABLE_DELTA_LOG
// 确保 DELTA 布局满足“首两个字节即 tag”
static_assert(offsetof(WalDeltaHeader, type) == 0,
              "WalDeltaHeader::type must be the first field");
static_assert(std::is_same<decltype(WalDeltaHeader{}.type), uint16_t>::value,
              "WalDeltaHeader::type must be uint16_t");
#endif

// 统一解析并应用一段连续缓冲 [base0, base0+batch)
static size_t parseApplySpan(CkptLog* self,
                             const unsigned char* base0,
                             size_t batch,
                             PmemInodePool* pmemInodePool) {
    size_t consumed = 0;

    while (consumed + sizeof(uint16_t) <= batch) {
        const unsigned char* base = base0 + consumed;
        uint16_t tag = *reinterpret_cast<const uint16_t*>(base);

#if ENABLE_DELTA_LOG
        if (tag == WAL_LOG_TYPE_DELTA) {
            if (consumed + sizeof(WalDeltaHeader) > batch) break;
            const auto* dh = reinterpret_cast<const WalDeltaHeader*>(base);
            if (dh->count == 0 || dh->count > fanout) break;

            size_t raw_sz   = sizeof(WalDeltaHeader) + dh->count * sizeof(WalDeltaEntry);
            size_t entry_sz = PmemManager::align_uint_to_cacheline(static_cast<unsigned>(raw_sz));
            if (entry_sz == 0 || consumed + entry_sz > batch) break;

            const auto* entries = reinterpret_cast<const WalDeltaEntry*>(dh + 1);
            Inode* inode = pmemInodePool->at(dh->inode_id);
            self->applyDeltaEntries(inode, entries, dh->count,
                                    dh->last_index, dh->next, dh->parent_id);

            consumed += entry_sz;
            continue;
        }
#endif
        if (tag == WAL_LOG_TYPE_FULL) {
            if (consumed + sizeof(uint16_t) + sizeof(log_entry_hdr) > batch) break;
            const auto* fh = reinterpret_cast<const log_entry_hdr*>(base + sizeof(uint16_t));
            if (fh->count < 0 || fh->count > fanout) break;

            size_t raw_sz   = sizeof(uint16_t) + sizeof(log_entry_hdr)
                            + static_cast<size_t>(fh->count) * sizeof(nvm_log_entry_t);
            size_t entry_sz = PmemManager::align_uint_to_cacheline(static_cast<unsigned>(raw_sz));
            if (entry_sz == 0 || consumed + entry_sz > batch) break;

            const auto* entries = reinterpret_cast<const nvm_log_entry_t*>(
                reinterpret_cast<const unsigned char*>(fh) + sizeof(log_entry_hdr));

            Inode* inode = pmemInodePool->at(fh->id);
            inode->hdr.last_index = fh->last_index;
            inode->hdr.next       = fh->next;
            inode->hdr.level      = fh->level;
            inode->hdr.parent_id  = fh->parent_id;
            

            if(fh->level > self->current_highest_level) {
                if(fh->next != -1 &&  fh->next!= 0 && !(fh->next >= MAX_LEVEL && fh->next < 2 * MAX_LEVEL)) {
                    self->current_highest_level = fh->level;
                    Inode *superNode = pmemInodePool->at(MAX_NODES - 1);
                    superNode->hdr.level = self->current_highest_level;
                    PmemManager::flushNoDrain(1, &superNode->hdr.level, sizeof(Inode));
                }
            }
            if(fh->id > self->current_inode_idx) {
                self->inode_count_on_each_level[fh->level]++;
                self->current_inode_idx = fh->id;
                Inode *superNode = pmemInodePool->at(MAX_NODES - 1);
                superNode->hdr.next = self->current_inode_idx;
                PmemManager::flushNoDrain(1, &superNode->hdr.next, sizeof(Inode));
            }

            for (int i = 0; i < fh->count; ++i) {
                int gi = entries[i].gp_idx;
                inode->gps[gi].key           = entries[i].key;
                inode->gps[gi].value         = entries[i].value;
                inode->gps[gi].covered_nodes = entries[i].covered_nodes;
            }
            PmemManager::flushNoDrain(1, inode, sizeof(Inode)); //1: INDEXPOOL

            consumed += entry_sz;
            continue;
        }

        // 未知 tag 或不完整，等待后续批次
        break;
    }

    return consumed;
}

int CkptLogNVM::init(root_obj *root, size_t maxSize) {
    bool isCreate;
    bool ret = PmemManager::createOrOpenPool(CKPLOGPOOL, fileName.c_str(), maxSize+1024*1024*1024, (void **)&root, isCreate);
    if (!ret) {
        std::cout << "Failed to create or open pool: " << fileName << std::endl;
        return -1;
    } 

    // To allocate the checkpoint log. 1. allocate memory. 2. cast into ckp_entry 3. pot them into vector.
    PMEMobjpool *pop = (PMEMobjpool *)PmemManager::getPoolStartAddress(CKPLOGPOOL);
    if(isCreate) {
        int ret_val = pmemobj_alloc(pop, &root->ptr[0], maxSize+L1_CACHE_LINE_SIZE, 0, NULL, NULL);
        if (ret_val) {
            std::cout << "Failed to allocate memory for ckpt_log" << std::endl;
            return -1;
        }
        this->_buf= static_cast<unsigned char *> (pmemobj_direct(root->ptr[0]));
        this->buf = PmemManager::align_ptr_to_cacheline((void *)this->_buf);
        PmemManager::flushToNVM(CKPLOGPOOL, (char *)(this->buf), maxSize+L1_CACHE_LINE_SIZE);
        return 0;
    }else {
        this->_buf= static_cast<unsigned char *> (pmemobj_direct(root->ptr[0]));
        this->buf = PmemManager::align_ptr_to_cacheline((void *)this->_buf);
        return 0;
    }
}

// 构造函数
#if ENABLE_PMEM_STATS
CkptLog::CkptLog(size_t logSize, int current_highest_level, ValueList *va_list)
    : retry_count(0), current_highest_level(current_highest_level), valueList(va_list),
      ckptlog(new CkptLogNVM(logSize)) {
#else
CkptLog::CkptLog(size_t logSize)
    : retry_count(0),
      ckptlog(new CkptLogNVM(logSize)) {
#endif
    a_consumed_start.v.store(ckptlog->start, std::memory_order_relaxed);
    a_durable_end.v.store(ckptlog->start_persistent, std::memory_order_relaxed);
    a_produced_end.v.store(ckptlog->end_persistent, std::memory_order_relaxed);
    current_inode_idx = 0;
    current_highest_level = 0;
    inode_count_on_each_level.reserve(MAX_LEVEL);
}

// 析构函数
CkptLog::~CkptLog() {
    delete ckptlog;
}

//get nvm_log_entry_t at inde
void CkptLog::enq(dram_log_entry_t *entry)
{
    if (!entry) return;

    // 计算条目长度：2字节类型标记 + 头 + 负载，对齐到 cacheline
    const size_t tag_sz   = sizeof(uint16_t); // WalLogType
    const size_t hdr_sz   = sizeof(log_entry_hdr);
    const size_t body_sz  = entry->getPayLoadSize(); // = sizeof(nvm_log_entry_t) * count
    const size_t used     = tag_sz + hdr_sz + body_sz;
    const size_t entry_sz = PmemManager::align_uint_to_cacheline(static_cast<unsigned int>(used));

    log_entry_hdr *slot = nullptr;
    {
        std::unique_lock<std::shared_mutex> lk(mtx);
        slot = nvm_log_enq(entry_sz);
        if (!slot) {
            throw std::runtime_error("ckpt log full");
        }
    }

    // 起始地址（可按字节写入）
    auto *base = reinterpret_cast<unsigned char *>(slot);

    // 1) 写类型标记：FULL
    *reinterpret_cast<uint16_t *>(base) = WAL_LOG_TYPE_FULL;

    // 2) 写 header（位于标记之后）
    auto *hdr = reinterpret_cast<log_entry_hdr *>(base + tag_sz);
    initLogEntryHeaderFromDramLogEntry(hdr, entry); // 复用现有函数初始化头部（包含 count 等）

    // 3) 写 payload（nvm_log_entry_t[count]）
    auto *out = reinterpret_cast<nvm_log_entry_t *>(reinterpret_cast<unsigned char *>(hdr) + sizeof(log_entry_hdr));
    for (int i = 0; i < entry->hdr.count; ++i) {
        out[i].gp_idx        = entry->gp_idx[i];
        out[i].key           = entry->key[i];
        out[i].value         = entry->value[i];
        out[i].covered_nodes = entry->covered_nodes[i];
    }

    // 4) 尾部对齐填充
    if (entry_sz > used) {
        std::memset(base + used, 0, entry_sz - used);
    }
    
    // 5) 推进生产游标
    a_produced_end.v.fetch_add(entry_sz, std::memory_order_release);
}

#ifndef ENABLE_DELTA_LOG
#define ENABLE_DELTA_LOG 1
#endif

#if ENABLE_DELTA_LOG
void CkptLog::enqDelta(int32_t inode_id,
                       int32_t last_index,
                       int32_t next,
                       int32_t parent_id,
                       const WalDeltaEntry *entries,
                       size_t entry_count)
{
    if (!appendDeltaLog(inode_id, last_index, next, parent_id, entries, entry_count)) {
        throw std::runtime_error("ckpt log full (delta)");
    }
}

bool CkptLog::appendDeltaLog(int32_t inode_id,
                             int32_t last_index,
                             int32_t next,
                             int32_t parent_id,
                             const WalDeltaEntry *entries,
                             size_t entry_count)
{
    if (!entries || entry_count == 0) return true;

    size_t payload_bytes = sizeof(WalDeltaHeader) + entry_count * sizeof(WalDeltaEntry);
    size_t entry_size = PmemManager::align_uint_to_cacheline((unsigned)payload_bytes);

    log_entry_hdr *slot = nullptr;
    {
        std::unique_lock<std::shared_mutex> lk(mtx);
        slot = nvm_log_enq(entry_size);
        if (!slot) return false;
    }

    auto *hdr = reinterpret_cast<WalDeltaHeader *>(slot);
    hdr->type       = WAL_LOG_TYPE_DELTA;
    hdr->count      = (uint16_t)entry_count;
    hdr->inode_id   = inode_id;
    hdr->last_index = last_index;
    hdr->next       = next;
    hdr->parent_id  = parent_id;

    auto *delta_entries = reinterpret_cast<WalDeltaEntry *>(hdr + 1);
    std::memcpy(delta_entries, entries, entry_count * sizeof(WalDeltaEntry));

    size_t used = sizeof(WalDeltaHeader) + entry_count * sizeof(WalDeltaEntry);
    if (entry_size > used) {
        std::memset(reinterpret_cast<char *>(hdr) + used, 0, entry_size - used);
    }
    
    a_produced_end.v.fetch_add(entry_size, std::memory_order_release);
    
    return true;
}

void CkptLog::applyDeltaEntries(Inode *inode,
                                const WalDeltaEntry *entries,
                                size_t entry_count,
                                int32_t new_last_index,
                                int32_t new_next,
                                int32_t new_parent_id)
{
    if (!inode || !entries) return;
    for (size_t i = 0; i < entry_count; ++i) {
        int16_t s = entries[i].slot;
        if (s < 0 || s >= fanout) continue;
        auto &gp = inode->gps[s];
        gp.key = entries[i].key;
        gp.value = entries[i].value;
        gp.covered_nodes = entries[i].covered;
        PmemManager::flushNoDrain(1, &gp, sizeof(gp)); // i is INDEXPOOL
    }
    if (new_last_index != WAL_META_KEEP) {
        inode->hdr.last_index = new_last_index;
        PmemManager::flushNoDrain(1, &inode->hdr.last_index, sizeof(inode->hdr.last_index));
    }
    if (new_next != WAL_META_KEEP) {
        inode->hdr.next = new_next;
        PmemManager::flushNoDrain(1, &inode->hdr.next, sizeof(inode->hdr.next));
    }
    if (new_parent_id != WAL_META_KEEP) {
        inode->hdr.parent_id = new_parent_id;
        PmemManager::flushNoDrain(1, &inode->hdr.parent_id, sizeof(inode->hdr.parent_id));
    }
}
#endif // ENABLE_DELTA_LOG

log_entry_hdr *CkptLog::nvm_log_enq(size_t entry_size)
{
    log_entry_hdr *log_entry_hdr;
    if (entry_size > ckptlog->log_size) {
        cout << "Object size is greater than log size: " << entry_size << endl;
        return NULL;
    }
    if ((ckptlog->end + entry_size) - ckptlog->start > ckptlog->log_size) {
        std::cout << "Log is full" << std::endl;
        return NULL;
    }
    log_entry_hdr = nvm_log_at(ckptlog->end);
    ckptlog->current_update = ckptlog->end;
#ifdef LOG_DEBUG
    size_t old_end = ckptlog->end;
#endif
    ckptlog->end = ckptlog->end + entry_size;
#ifdef LOG_DEBUG
    cout << "Log enq, old_end: " << old_end << " ckptlog->start: " << ckptlog->start
         << " ckptlog->end: "<< ckptlog->end <<" ckptlog->current_update "<< ckptlog->current_update
         <<" ckptlog->start_persistent " << ckptlog->start_persistent
         << " ckptlog->end_persis " << ckptlog->end_persistent << endl;
#endif
    return log_entry_hdr;
}

// 新：强制把尚未持久化的 [a_durable_end, a_produced_end) 全部刷到 NVM（忽略阈值）
void CkptLog::forcePersist()
{
    for (;;) {
        size_t durable  = a_durable_end.v.load(std::memory_order_acquire);
        size_t produced = a_produced_end.v.load(std::memory_order_acquire);
        if (produced <= durable) break;
        size_t len = produced - durable;

        // 处理环形两段
        size_t off       = nvm_log_index(durable);
        size_t tail_left = ckptlog->log_size - off;
        if (len <= tail_left) {
            PmemManager::flushNoDrain(CKPLOGPOOL, nvm_log_at(durable), len);
        } else {
            PmemManager::flushNoDrain(CKPLOGPOOL, nvm_log_at(durable), tail_left);
            PmemManager::flushNoDrain(CKPLOGPOOL, nvm_log_at(0), len - tail_left);
        }
        PmemManager::drain(CKPLOGPOOL);

        // 持久化元数据（可恢复 durable 边界）
        ckptlog->end_persistent = produced;
        PmemManager::flushNoDrain(CKPLOGPOOL, &ckptlog->end_persistent, sizeof(ckptlog->end_persistent));
        PmemManager::drain(CKPLOGPOOL);

        a_durable_end.v.store(produced, std::memory_order_release);
    }
}

// 新：使用新游标强制回放并可选重置
void CkptLog::forceReclaim(PmemInodePool *pmemInodePool)
{
    // 先把 durable 推到 produced，避免解析首条不完整
    forcePersist();

    for (;;) {
        size_t consumed = a_consumed_start.v.load(std::memory_order_acquire);
        size_t durable  = a_durable_end.v.load(std::memory_order_acquire);
        if (consumed >= durable) break;

        // 若生产继续推进，先把 durable 补到最新
        size_t produced = a_produced_end.v.load(std::memory_order_acquire);
        if (produced > durable) {
            forcePersist();
            durable = a_durable_end.v.load(std::memory_order_acquire);
            if (consumed >= durable) break;
        }

        size_t window = durable - consumed;
        // 优先尝试最多一圈，减少跨尾复杂度
        size_t got = reclaimBatch(pmemInodePool, std::min(window, ckptlog->log_size));
        if (got == 0) {
            // 退一步：仅尝试吃到环尾，避免跨尾解析敏感
            size_t idx     = nvm_log_index(consumed);
            size_t to_tail = ckptlog->log_size - idx;
            size_t slice   = std::min(window, to_tail);
            if (slice > 0) got = reclaimBatch(pmemInodePool, slice);
        }

        if (got == 0) {
            // 仍无进展：轻量让出 CPU，等待可能的持久/生产推进后重试
            std::this_thread::yield();
        }
    }

    // 仅在确实清空 backlog 时再重置环
    size_t c = a_consumed_start.v.load(std::memory_order_acquire);
    size_t d = a_durable_end.v.load(std::memory_order_acquire);
    if (c < d) return;

    std::unique_lock<std::shared_mutex> lk(mtx, std::try_to_lock);
    if (lk.owns_lock()) {
        ckptlog->start = ckptlog->end = ckptlog->current_update = 0;
        ckptlog->start_persistent = ckptlog->end_persistent = 0;

        a_consumed_start.v.store(0, std::memory_order_relaxed);
        a_durable_end.v.store(0, std::memory_order_relaxed);
        a_produced_end.v.store(0, std::memory_order_relaxed);

        PmemManager::flushNoDrain(CKPLOGPOOL, ckptlog, sizeof(*ckptlog));
        PmemManager::drain(CKPLOGPOOL);
    }
}

size_t CkptLog::reclaimBatch(PmemInodePool *pmemInodePool, size_t max_bytes)
{
    //return 0;
    size_t start   = a_consumed_start.v.load(std::memory_order_acquire);
    size_t durable = a_durable_end.v.load(std::memory_order_acquire);
    if (start >= durable) return 0;

    size_t window = durable - start;
    size_t batch  = std::min(window, max_bytes);
    if (!batch) return 0;

    // 关键：限制单次批量不超过环大小，确保跨尾最多一次拼接
    batch = std::min(batch, ckptlog->log_size);

    const size_t idx  = nvm_log_index(start);
    const size_t tail = ckptlog->log_size - idx;

    size_t consumed = 0;

    if (batch <= tail) {
        // 不跨尾：零拷贝解析
        const unsigned char* base = reinterpret_cast<const unsigned char*>(nvm_log_at(start));
        consumed = parseApplySpan(this, base, batch, pmemInodePool);
    } else {
        // 跨尾：拷贝到临时缓冲后统一解析（batch 已被夹到 <= log_size）
        std::vector<unsigned char> buf(batch);
        std::memcpy(buf.data(),           nvm_log_at(start), tail);
        std::memcpy(buf.data() + tail,    nvm_log_at(0),     batch - tail);
        consumed = parseApplySpan(this, buf.data(), batch, pmemInodePool);
    }

    if (consumed == 0) return 0;

    PmemManager::drain(1); // 1: INDEXPOOL
    a_consumed_start.v.fetch_add(consumed, std::memory_order_acq_rel);
    return consumed;
}

void CkptLog::reclaim(double dramSearchEfficincy, long vnode_count, PmemInodePool *pmemInodePool)
{
    try {
        size_t q = getLogQueueSize();
        double pmemSearchEfficiency = calculatePmemSearchEfficiency(vnode_count);
        double threshold = pmemSearchEfficiency / dramSearchEfficincy;
        if (q < RECLAIM_THRESHOLD && retry_count < RECLAIM_RETRY_THRESHOLD && threshold <= 1.2) {
            retry_count++;
            return;
        }
        retry_count = 0;

        const size_t BATCH_BYTES = std::max<size_t>(PERSISTENT_THRESHOLD, RECLAIM_THRESHOLD);
        while (reclaimBatch(pmemInodePool, BATCH_BYTES) > 0) { /* loop */ }
#if ENABLE_PMEM_STATS
        {
            std::shared_lock<std::shared_mutex> lk(pmemInodePool->stats_mtx);
            int i = current_highest_level;
            int vnode_count = valueList->pmemVnodePool->getCurrentIdx();
            pmemInodePool->printStats(i, vnode_count);
        }
    cout << "Reclaim finished. current_highest_level: " << current_highest_level << " current_inode_idx: " << current_inode_idx << endl;
#endif
    } catch (std::exception &e) {
        std::cout << "Exception in reclaim: " << e.what() << std::endl;
    }
}

// uniform ring index: prefer &mask if mask = log_size - 1;
unsigned int CkptLog::nvm_log_index(unsigned long idx)
{
    const size_t log_size = ckptlog->log_size;
    const size_t mask     = ckptlog->mask;

    if (log_size && (log_size & (log_size - 1)) == 0 && mask == (log_size - 1)) {
        return static_cast<unsigned int>(idx & mask);
    }
    if (log_size) {
        return static_cast<unsigned int>(idx % log_size);
    }
    // fallback（未初始化时避免崩溃）
    return static_cast<unsigned int>(idx);
}

// 通过环形索引定位 NVM 中的条目头地址
log_entry_hdr *CkptLog::nvm_log_at(size_t index)
{
    const unsigned int off = nvm_log_index(index);
    auto *base = const_cast<unsigned char *>(ckptlog->buf); // buf 为持久化起始
    return reinterpret_cast<log_entry_hdr *>(base + off);
}

// 基于“已持久化边界”的无锁判空
bool CkptLog::isLogEmpty()
{
    size_t c = a_consumed_start.v.load(std::memory_order_acquire);
    size_t d = a_durable_end.v.load(std::memory_order_acquire);
    return c >= d;
}

// 返回已持久化的可消费区间长度
size_t CkptLog::getLogQueueSize()
{
    size_t c = a_consumed_start.v.load(std::memory_order_acquire);
    size_t d = a_durable_end.v.load(std::memory_order_acquire);
    return (d > c) ? (d - c) : 0;
}

double CkptLog::calculatePmemSearchEfficiency(long vnode_count)
{
    int cur_level = current_highest_level;
    int max_level_idx = cur_level - 1;
    double E_index = 0.0;
    if(max_level_idx >=0) {
        E_index += 1.0;
    }

    for(int i = max_level_idx; i > 0; i--) {
        double upper = static_cast<double>(inode_count_on_each_level[i]);
        double lower = static_cast<double>(inode_count_on_each_level[i-1]);
        if(upper > 0.0) {
            double fanout = lower / upper;
            E_index += fanout / 2.0;
        }
    }

    double E_data = 0.0;
    if (vnode_count > 0 && inode_count_on_each_level[0] > 0) {
        double Vtotal = static_cast<double>(vnode_count);
        double avg_vnodes_per_inode = Vtotal / static_cast<double>(inode_count_on_each_level[0]);
        E_data = avg_vnodes_per_inode / 2.0;
    }

    double E_search = E_index + E_data;
    return E_search;
}

bool CkptLog::flushOnce()
{
    // 防止并发 flush
    if (flush_busy.test_and_set(std::memory_order_acq_rel)) return false;

    size_t durable  = a_durable_end.v.load(std::memory_order_acquire);
    size_t produced = a_produced_end.v.load(std::memory_order_acquire);
    if (produced <= durable) {
        flush_busy.clear(std::memory_order_release);
        return false;
    }

    size_t len = produced - durable;

    // 阈值门控（可调）：不足 PERSISTENT_THRESHOLD 可暂缓
    if (len < PERSISTENT_THRESHOLD) {
        flush_busy.clear(std::memory_order_release);
        return false;
    }

    // 处理环形两段 flush
    size_t off = nvm_log_index(durable);
    size_t tail_bytes = ckptlog->log_size - off;
    if (len <= tail_bytes) {
        PmemManager::flushNoDrain(CKPLOGPOOL, nvm_log_at(durable), len);
    } else {
        PmemManager::flushNoDrain(CKPLOGPOOL, nvm_log_at(durable), tail_bytes);
        PmemManager::flushNoDrain(CKPLOGPOOL, nvm_log_at(0), len - tail_bytes);
    }
    // 单次 drain
    PmemManager::drain(CKPLOGPOOL);

    // 推进 durable_end（release，让回放线程看到完整持久内容）
    a_durable_end.v.store(produced, std::memory_order_release);

    // 可选：镜像持久元数据（不阻塞）
    std::unique_lock<std::shared_mutex> lk(mtx, std::try_to_lock);
    if (lk.owns_lock()) {
        ckptlog->end_persistent   = produced;
        ckptlog->start_persistent = a_consumed_start.v.load(std::memory_order_relaxed);
    }

    flush_busy.clear(std::memory_order_release);
    return true;
}

void CkptLog::waitDurable(size_t lsn)
{
    for (;;) {
        size_t d = a_durable_end.v.load(std::memory_order_acquire);
        if (d >= lsn) break;
        // 轻量自旋或 sleep/yield
        std::this_thread::yield();
    }
}

// 成员函数实现：从 DRAM 日志条目头复制必要字段
void CkptLog::initLogEntryHeaderFromDramLogEntry(log_entry_hdr *dst,
                                                 const dram_log_entry_t *src)
{
    // 基本防御
    if (!dst || !src) return;
    // 复制头字段
    dst->id         = src->hdr.id;
    dst->count      = src->hdr.count;
    dst->last_index = src->hdr.last_index;
    dst->next       = src->hdr.next;
    dst->level      = src->hdr.level;
    dst->parent_id  = src->hdr.parent_id;
    assert(dst->id >=0);
}

size_t CkptLog::getDurableGap() const
{
    size_t c = a_consumed_start.v.load(std::memory_order_acquire);
    size_t d = a_durable_end.v.load(std::memory_order_acquire);
    return d > c ? (d - c) : 0;
}

size_t CkptLog::getProducedGap() const
{
    size_t d = a_durable_end.v.load(std::memory_order_acquire);
    size_t p = a_produced_end.v.load(std::memory_order_acquire);
    return p > d ? (p - d) : 0;
}

size_t CkptLog::getBacklogGap() const
{
    size_t c = a_consumed_start.v.load(std::memory_order_acquire);
    size_t p = a_produced_end.v.load(std::memory_order_acquire);
    return p > c ? (p - c) : 0;
}

bool CkptLog::tryFlushOnce()
{
    // 封装内部 flushOnce()；按内部阈值决定是否执行
    return flushOnce();
}

size_t CkptLog::suggestReclaimBatchBytes() const
{
#ifdef PERSISTENT_THRESHOLD
    size_t a = PERSISTENT_THRESHOLD;
#else
    size_t a = 64 * 1024;
#endif
#ifdef RECLAIM_THRESHOLD
    size_t b = RECLAIM_THRESHOLD;
#else
    size_t b = 64 * 1024;
#endif
    return a > b ? a : b;
}

unsigned char* CkptLog::reserveChunk(size_t total_bytes_aligned) {
    if (!total_bytes_aligned) return nullptr;
    log_entry_hdr* slot = nullptr;
    {
        std::unique_lock<std::shared_mutex> lk(mtx);
        slot = nvm_log_enq(total_bytes_aligned);
        if (!slot) return nullptr;
    }
    return reinterpret_cast<unsigned char*>(slot);
}

void CkptLog::commitChunk(size_t total_bytes_aligned) {
    a_produced_end.v.fetch_add(total_bytes_aligned, std::memory_order_release);
}

void CkptLog::enqBatch(const std::vector<dram_log_entry_t*>& entries) {
    if (entries.empty()) return;

    std::vector<size_t> sz(entries.size());
    size_t total = 0;
    for (size_t i = 0; i < entries.size(); ++i) {
        const size_t used = sizeof(uint16_t) + sizeof(log_entry_hdr) + entries[i]->getPayLoadSize();
        size_t e = PmemManager::align_uint_to_cacheline(static_cast<unsigned>(used));
        sz[i] = e; total += e;
    }

    unsigned char* base = reserveChunk(total);
    if (!base) {
        // 回退逐条
        for (auto* e : entries) enq(e);
        return;
    }

    size_t off = 0;
    for (size_t i = 0; i < entries.size(); ++i) {
        unsigned char* cur = base + off;

        // tag
        *reinterpret_cast<uint16_t*>(cur) = WAL_LOG_TYPE_FULL;

        // header
        auto* hdr = reinterpret_cast<log_entry_hdr*>(cur + sizeof(uint16_t));
        initLogEntryHeaderFromDramLogEntry(hdr, entries[i]);

        // payload
        auto* out = reinterpret_cast<nvm_log_entry_t*>(
            reinterpret_cast<unsigned char*>(hdr) + sizeof(log_entry_hdr));
        for (int k = 0; k < entries[i]->hdr.count; ++k) {
            out[k].gp_idx        = entries[i]->gp_idx[k];
            out[k].key           = entries[i]->key[k];
            out[k].value         = entries[i]->value[k];
            out[k].covered_nodes = entries[i]->covered_nodes[k];
        }

        // padding
        const size_t used = sizeof(uint16_t) + sizeof(log_entry_hdr)
                          + entries[i]->hdr.count * sizeof(nvm_log_entry_t);
        if (sz[i] > used) std::memset(cur + used, 0, sz[i] - used);

        off += sz[i];
    }

    commitChunk(total);
}

void CkptLog::enqDeltaBatch(const std::vector<DeltaPack>& packs) {
    if (packs.empty()) return;

    std::vector<size_t> sz(packs.size());
    size_t total = 0;
    for (size_t i = 0; i < packs.size(); ++i) {
        const size_t used = sizeof(WalDeltaHeader)
                          + packs[i].entries.size() * sizeof(WalDeltaEntry);
        size_t e = PmemManager::align_uint_to_cacheline(static_cast<unsigned>(used));
        sz[i] = e; total += e;
    }

    unsigned char* base = reserveChunk(total);
    if (!base) {
        // 回退逐条
        for (const auto& p : packs) {
            appendDeltaLog(p.hdr.inode_id,
                           p.hdr.last_index,
                           p.hdr.next,
                           p.hdr.parent_id,
                           p.entries.data(),
                           p.entries.size());
        }
        return;
    }

    size_t off = 0;
    for (size_t i = 0; i < packs.size(); ++i) {
        unsigned char* cur = base + off;

        auto* hdr = reinterpret_cast<WalDeltaHeader*>(cur);
        *hdr = packs[i].hdr;
        hdr->type  = WAL_LOG_TYPE_DELTA;
        hdr->count = (uint16_t)packs[i].entries.size();

        auto* out = reinterpret_cast<WalDeltaEntry*>(hdr + 1);
        if (!packs[i].entries.empty()) {
            std::memcpy(out, packs[i].entries.data(),
                        packs[i].entries.size() * sizeof(WalDeltaEntry));
        }

        const size_t used = sizeof(WalDeltaHeader)
                          + packs[i].entries.size() * sizeof(WalDeltaEntry);
        if (sz[i] > used) std::memset(cur + used, 0, sz[i] - used);

        off += sz[i];
    }

    commitChunk(total);
}

// ===== Batcher 实现 =====
void CkptLog::Batcher::addFull(dram_log_entry_t* e) {
    if (!e) return;
    const size_t used = sizeof(uint16_t) + sizeof(log_entry_hdr) + e->getPayLoadSize();
    const size_t aligned = PmemManager::align_uint_to_cacheline(static_cast<unsigned>(used));
    Event ev{};
    ev.kind = Kind::Full;
    ev.seq  = s_seq_++;
    ev.f    = EvFull{e, aligned};

    events_.push_back(ev);
    bytes_est_ += aligned;
    maybeFlush();
}

void CkptLog::Batcher::addDeltaSlot(int32_t inode_id,
                                    int32_t last_index,
                                    int32_t next,
                                    int32_t parent_id,
                                    int16_t slot,
                                    const Key_t& key,
                                    const Val_t& value,
                                    int16_t covered) {
#if ENABLE_DELTA_LOG
    WalDeltaHeader hdr{};
    hdr.type       = WAL_LOG_TYPE_DELTA;
    hdr.inode_id   = inode_id;
    hdr.last_index = last_index;
    hdr.next       = next;
    hdr.parent_id  = parent_id;
    hdr.count      = 1;

    WalDeltaEntry de{};
    de.slot    = slot;
    de.key     = key;
    de.value   = value;
    de.covered = covered;

    // 估算大小：header+1entry（若 run 聚合，会更小）
    const size_t used = sizeof(WalDeltaHeader) + sizeof(WalDeltaEntry);
    const size_t aligned = PmemManager::align_uint_to_cacheline(static_cast<unsigned>(used));

    Event ev{};
    ev.kind = Kind::Delta;
    ev.seq  = s_seq_++;
    ev.d    = EvDelta{hdr, de, aligned};

    events_.push_back(ev);
    bytes_est_ += aligned;
    maybeFlush();
#else
    (void)inode_id;(void)last_index;(void)next;(void)parent_id;
    (void)slot;(void)key;(void)value;(void)covered;
#endif
}

void CkptLog::Batcher::maybeFlush() {
    bool hit_cnt   = events_.size() >= kMaxEntries;
    bool hit_bytes = bytes_est_ >= kMaxBytes;
    bool hit_time  = (Clock::now() - last_flush_) >= std::chrono::nanoseconds{kMaxDelayNs};
    if (hit_cnt || hit_bytes || hit_time) flush();
}

void CkptLog::Batcher::flush() {
    std::vector<Event> evs = std::move(events_);
    events_.clear();
    bytes_est_  = 0;
    last_flush_ = Clock::now();
    if (evs.empty()) return;

    // 1) 保持捕获顺序：events_ 已按 seq 追加（单线程），无需排序
    //    合并为多个“run”，相同类型相邻事件合并提交；DELTA run 内再按 inode 聚合为 pack
    size_t i = 0, n = evs.size();
    while (i < n) {
        const Kind k = evs[i].kind;
        size_t j = i + 1;
        while (j < n && evs[j].kind == k) ++j;

        if (k == Kind::Full) {
            // 收集 [i, j) 为 FULL 组
            std::vector<dram_log_entry_t*> group;
            group.reserve(j - i);
            for (size_t t = i; t < j; ++t) group.push_back(evs[t].f.e);
            owner_->enqBatch(group);
        } else { // Kind::Delta
            // 将 [i, j) 内连续、inode 元字段相同的 DELTA 合并成 pack
            std::vector<CkptLog::DeltaPack> packs;
            packs.reserve(j - i);
            size_t p = i;
            while (p < j) {
                DeltaPack pack{};
                pack.hdr = evs[p].d.hdr;
                pack.hdr.count = 0;
                // 合并同一 inode 元字段（inode_id/last_index/next/parent_id）
                size_t q = p;
                for (; q < j; ++q) {
                    const auto& cur = evs[q].d;
                    if (cur.hdr.inode_id   != pack.hdr.inode_id ||
                        cur.hdr.last_index != pack.hdr.last_index ||
                        cur.hdr.next       != pack.hdr.next ||
                        cur.hdr.parent_id  != pack.hdr.parent_id) {
                        break; // 到了不同 inode 或不同元版本，结束本 pack
                    }
                    pack.entries.push_back(cur.entry);
                }
                pack.hdr.count = static_cast<uint16_t>(pack.entries.size());
                packs.push_back(std::move(pack));
                p = q;
            }
            owner_->enqDeltaBatch(packs);
        }

        i = j;
    }

    evs.clear();
    bytes_est_  = 0;
    last_flush_ = Clock::now();
}

