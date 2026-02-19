# 恢复 dwd_shareholder_num_latest 表数据计划

## 1. 当前情况分析

### 1.1 问题描述
- **误操作**：在执行数据迁移脚本时，使用了 `TRUNCATE TABLE dwd_shareholder_num_latest` 清空了目标表
- **当前状态**：`dwd_shareholder_num_latest` 表数据量为 0
- **影响**：该表中的历史数据丢失

### 1.2 数据来源分析
- **源表**：`ods_akshare_shareholder_em_history`（数据完整）
- **目标表**：`dwd_shareholder_num_latest`（已清空）
- **关系**：`dwd_shareholder_num_latest` 的数据是从 `ods_akshare_shareholder_em_history` 加工而来

### 1.3 表结构对比

**ods_akshare_shareholder_em_history 表：**
- id (bigint, PK, auto_increment)
- ymd (date)
- stock_code (varchar(50))
- stock_name (varchar(50))
- total_sh (bigint)
- avg_share (float)
- pct_of_total_sh (float)
- pct_of_avg_sh (float)

**dwd_shareholder_num_latest 表：**
- ymd (date)
- stock_code (varchar(100))
- stock_name (varchar(50))
- total_sh (double)
- avg_share (double(10,4))
- pct_of_total_sh (double(10,4))
- pct_of_avg_sh (double(10,4))

**差异：**
- ods表有id自增主键，dwd表没有
- 字段类型略有不同（float vs double）
- 字段数量相同（都是7个业务字段）

## 2. 恢复方案

### 2.1 方案选择

**方案：从 ods_akshare_shareholder_em_history 表重新插入**

**理由：**
1. dwd表的数据来源于ods表
2. ods表数据完整未受影响
3. 可以通过SQL直接插入恢复

### 2.2 恢复步骤

#### 步骤 1：确认源表数据完整性
- 检查 `ods_akshare_shareholder_em_history` 表的数据量
- 确认数据日期范围
- 确认 `pct_of_avg_sh` 字段已正确更新

#### 步骤 2：准备目标表
- 确认 `dwd_shareholder_num_latest` 表结构正确
- 确认表为空（已清空）

#### 步骤 3：执行数据插入
- 从 `ods_akshare_shareholder_em_history` 查询数据
- 插入到 `dwd_shareholder_num_latest`
- 指定列名，避免id字段

#### 步骤 4：验证恢复结果
- 检查目标表数据量
- 检查日期范围
- 抽样检查数据正确性

## 3. 详细执行计划

### 3.1 数据范围确认

**需要确认的问题：**

1. **日期范围**：
   - 用户要求：2026-02-11 之前的数据
   - 需要确认：是否包含 2026-02-11 当天？

2. **数据筛选条件**：
   - 是否所有股票都要迁移？
   - 是否有其他筛选条件？

3. **重复数据处理**：
   - 如果插入过程中出现重复数据如何处理？
   - 是否需要先删除再插入？

### 3.2 SQL 执行计划

```sql
-- 步骤 1：检查源表数据
SELECT 
    COUNT(*) as total_count,
    MIN(ymd) as min_date,
    MAX(ymd) as max_date
FROM ods_akshare_shareholder_em_history
WHERE ymd < '2026-02-11';

-- 步骤 2：插入数据
INSERT INTO dwd_shareholder_num_latest 
(ymd, stock_code, stock_name, total_sh, avg_share, pct_of_total_sh, pct_of_avg_sh)
SELECT 
    ymd, 
    stock_code, 
    stock_name, 
    total_sh, 
    avg_share, 
    pct_of_total_sh, 
    pct_of_avg_sh 
FROM ods_akshare_shareholder_em_history
WHERE ymd < '2026-02-11';

-- 步骤 3：验证结果
SELECT 
    COUNT(*) as total_count,
    MIN(ymd) as min_date,
    MAX(ymd) as max_date
FROM dwd_shareholder_num_latest;
```

## 4. 风险评估

### 4.1 潜在风险

1. **数据不一致风险**：
   - 如果ods表和dwd表之前有加工逻辑差异，直接复制可能不准确
   - 需要确认两个表的数据是否完全一致

2. **性能风险**：
   - ods表数据量较大（约780万条）
   - 一次性插入可能导致锁表或超时
   - 建议分批插入

3. **数据重复风险**：
   - 如果dwd表不是完全为空，可能导致重复数据
   - 需要先确认表状态

### 4.2 风险缓解措施

1. **分批插入**：
   - 按日期分批（如每月一批）
   - 或按股票代码分批

2. **先验证后插入**：
   - 先查询确认源表数据
   - 小批量测试插入
   - 验证无误后再全量插入

3. **事务控制**：
   - 使用事务，出现问题可以回滚

## 5. 执行前检查清单

- [ ] 确认源表 `ods_akshare_shareholder_em_history` 数据完整
- [ ] 确认目标表 `dwd_shareholder_num_latest` 已清空
- [ ] 确认日期范围（是否包含2026-02-11）
- [ ] 确认数据筛选条件
- [ ] 确认分批插入策略
- [ ] 准备验证脚本

## 6. 回滚方案

如果恢复过程中出现问题：

1. **立即停止**：停止当前插入操作
2. **清空表**：再次清空 `dwd_shareholder_num_latest` 表
3. **重新评估**：检查问题原因，调整方案后重新执行

## 7. 需要用户确认的事项

1. **日期范围确认**：
   - 是否包含 2026-02-11 当天的数据？
   - 还是严格小于 2026-02-11？

2. **数据筛选确认**：
   - 是否所有股票数据都要迁移？
   - 是否需要排除某些股票？

3. **执行方式确认**：
   - 一次性全量插入？
   - 还是分批插入（推荐）？

4. **验证方式确认**：
   - 如何验证数据恢复的正确性？
   - 需要检查哪些指标？

---

**计划制定完成，等待用户确认后执行。**
