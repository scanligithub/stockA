import pandas as pd
import json
import os

class QualityControl:
    def __init__(self):
        self.stats = {}
        self.diagnostics = {
            "missing_stocks": []  # 专门存放那 47 只“失踪人口”
        }

    def check_dataframe(self, df, name, check_cols=None, file_path=None):
        """
        对 DataFrame 进行通用质检统计
        """
        if df.empty:
            self.stats[name] = {"rows": 0, "count": 0, "error": "Empty DataFrame"}
            return

        # 1. 基础维度统计
        rows = len(df)
        unique_codes = df['code'].nunique() if 'code' in df.columns else 1
        
        # 2. 时间范围统计
        date_range = "---"
        if 'date' in df.columns:
            date_range = f"{df['date'].min()}~{df['date'].max()}"
            
        # 3. 文件大小统计
        size_mb = 0
        if file_path and os.path.exists(file_path):
            size_mb = round(os.path.getsize(file_path) / (1024 * 1024), 2)
            
        # 4. 异常值/空值检测 (简易版)
        anomaly_count = 0
        if check_cols:
            for col in check_cols:
                if col in df.columns:
                    anomaly_count += df[col].isna().sum()

        self.stats[name] = {
            "rows": rows,
            "stock_count": unique_codes,
            "date_range": date_range,
            "size_mb": size_mb,
            "anomaly_count": int(anomaly_count),
            "columns": ", ".join(df.columns.tolist())
        }

    def set_missing_analysis(self, missing_list):
        """
        存入缺失股票的详细信息
        missing_list: [{'code': '...', 'code_name': '...'}, ...]
        """
        self.diagnostics["missing_stocks"] = missing_list

    def get_report_dict(self):
        return {
            "file_stats": self.stats,
            "diagnostics": self.diagnostics
        }

    def save_report(self, path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.get_report_dict(), f, ensure_ascii=False, indent=2)

    def get_summary_md(self):
        """
        生成 GitHub Step Summary 使用的 Markdown 摘要
        """
        md = "## 📈 数据产物概览\n\n"
        md += "| 文件名 | 行数 | 标的数量 | 时间范围 | 大小 (MB) | 异常数 | 字段清单 |\n"
        md += "| --- | --- | --- | --- | --- | --- | --- |\n"
        
        for name, s in self.stats.items():
            md += f"| {name} | {s['rows']:,} | {s['stock_count']} | {s['date_range']} | {s['size_mb']} | {s['anomaly_count']} | {s['columns']} |\n"

        # 添加缺失股票诊断看板
        missing = self.diagnostics["missing_stocks"]
        if missing:
            md += f"\n\n## 🔍 数据缺失诊断 (发现 {len(missing)} 只标的未获取到行情)\n\n"
            md += "| 代码 | 名称 | 状态 |\n"
            md += "| --- | --- | --- |\n"
            # 仅展示前 50 只，防止 Markdown 过长
            for item in missing[:50]:
                md += f"| {item['code']} | {item['code_name']} | ❌ 缺失 |\n"
            if len(missing) > 50:
                md += f"| ... | ... | 以及其他 {len(missing)-50} 只 |\n"
            
            md += f"\n> **提示**：请检查这些股票是否处于长期停牌、退市或为北交所新上市标的。"
        else:
            md += "\n\n✅ **完整性检查**：所有目标股票均已成功获取数据。"
            
        return md
