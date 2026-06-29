---
license: mit
task_categories:
- feature-extraction
tags:
- legal
- finance
- stock-market
pretty_name: raw data
size_categories:
- 100M<n<1B
# 核心修改点：关闭自动预览，节省 1 倍存储空间，并防止由于预览转换产生的额外历史版本
viewer: false
---

# A-Share Quantitative Raw Data
This dataset contains historical K-line and sector data for the A-share market.
It is automatically synchronized from GitHub Releases and updated daily.

## Storage Management
- **Zero-History Sync**: The dataset is updated using `git push --force` to keep only the latest version.
- **Dataset Viewer**: Disabled to prevent duplicate Parquet storage in hidden branches.

## 产业链（Industry Chain）
大模型输出的这份两级嵌套 JSON 字典，**其结构完整、清洗彻底，已经完全具备了在 `stockA` 工程中落地并对接 Qlib 的条件**。

有了这份字典，Qlib 就可以通过以下两种方式将其转化为强大的量化交易信号：

---
### 第一种方式：直接作为特征输入（Qlib 经典多因子模型）

这是最快见效、也最容易实现的方式。Qlib 的模型（如 LightGBM、ALSTM）可以直接利用数值型的“产业链纯度敞口”进行截面训练。

#### 落地原理：
1. **保存字典**：在 `stockA` 目录下，新建 `utils/chain_dict.json`，把大模型吐出的这段 JSON 原封不动存进去。
2. **时序展开与对齐**：在 `merge_and_push.py` 中写一段简单的 Python/DuckDB 代码，读取这个 JSON，将个股的雪球主营明细转化为具体的 **Float32 产业链特征列**。
   * 比如，贵州茅台在 2023 年报披露后，它的 `exp_food_liquor`（白酒暴露度）会是 `0.95`。
   * 比亚迪的 `exp_ev_整车` 可能是 `0.70`，`exp_ev_电池` 可能是 `0.15`。
3. **Qlib 训练**：在生成 Qlib `.bin` 数据集时，将这些特征一并输入。
   * Qlib 模型能够轻易学习到非线性规律。例如：当“中游动力与储能电池”的整体动量（行业 Beta）起来，且某股票的 `exp_ev_整车` 敞口很大时，预测其未来的超额收益（Alpha）。

---

### 第二种方式：构建动态邻接矩阵（Qlib 图神经网络模型）

这是真正的“降维打击”。Qlib 拥有支持图关系挖掘的模型（如 **RSR - Relation Stock Ranking** 或 **TGC - Temporal Graph Convolution**）。这些模型需要一个关系矩阵（Adjacency Matrix）来描述股票之间的连通性。

#### 落地原理：
1. **定义宏观流向**：我们在代码中定义一条微观节点之间的传导规则。例如：
   * `上游锂矿与锂盐` $\rightarrow$ `中游动力与储能电池`
   * `芯片设计与制造` $\rightarrow$ `消费电子整车`
2. **时序图谱构建（无未来函数）**：
   在历史上的某一天 $t$：
   * 如果股票 $A$（赣锋锂业）的 `上游锂矿与锂盐` 敞口 $> 30\%$
   * 且股票 $B$（宁德时代）的 `中游动力与储能电池` 敞口 $> 30\%$
   * 我们就在 $t$ 日的邻接矩阵中，连一条从 $A \rightarrow B$ 的有向边：**$Edge(A, B) = 1$**。
3. **Qlib 图训练**：Qlib 会在每一个回测交易日加载当天最新的邻接矩阵。GNN 能够自适应地学习动量是如何通过这些有向边从上游（赣锋）传导给下游（宁德）的，从而捕捉到极佳的套利和对冲机会。

---

### 🏁 现状评估与下一步

目前，**数据提取（Top 1500）**和**产业链逻辑（2-Tier JSON）**这两个最难的堡垒已经被我们完全攻克。

接下来，我们需要将大模型输出的 `chain_dict.json` 翻译成 `stockA` 的工程语言。

**下一步的物理工作是：**
编写一段 Python 函数（可以嵌入到 `merge_and_push.py` 中），实现：
1. 读取 `chain_dict.json`；
2. 扫描 `all_stocks_mainbus_raw.parquet`；
3. **自动为 25 个宏观链或 100 多个微观节点，计算出每股每天无未来函数的 Float32 暴露度，并写入最终的 Parquet 宽表中。**

您准备好开启这最后一步的代码落地了吗？如果准备好了，我可以直接为您输出更新 `merge_and_push.py` 的具体方案。
