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
