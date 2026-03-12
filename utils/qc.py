import pandas as pd
import json
import os

class QualityControl:
    def __init__(self):
        self.stats = {}
        self.diagnostics = {"missing_stocks": []}

    def check_dataframe(self, df, name, check_cols=None, file_path=None):
        if df is None or df.empty:
            self.stats[name] = {"rows": 0, "stock_count": 0, "error": "Empty"}
            return

        rows = len(df)
        unique_codes = df['code'].nunique() if 'code' in df.columns else 0
        date_range = f"{df['date'].min()}~{df['date'].max()}" if 'date' in df.columns else "---"
        size_mb = round(os.path.getsize(file_path) / (1024 * 1024), 2) if file_path and os.path.exists(file_path) else 0
        
        anomaly_count = 0
        if check_cols:
            for col in check_cols:
                if col in df.columns: anomaly_count += df[col].isna().sum()

        self.stats[name] = {
            "rows": rows, "stock_count": unique_codes, "date_range": date_range,
            "size_mb": size_mb, "anomaly_count": int(anomaly_count),
            "columns": ", ".join(df.columns.tolist())
        }

    def set_missing_analysis(self, missing_list):
        self.diagnostics["missing_stocks"] = missing_list

    def get_report_dict(self):
        return {"file_stats": self.stats, "diagnostics": self.diagnostics}

    def get_summary_md(self):
        md = "## 📈 数据产物概览\n\n"
        md += "| 文件名 | 行数 | 标的数量 | 时间范围 | 大小 (MB) | 异常数 | 字段清单 |\n"
        md += "| --- | --- | --- | --- | --- | --- | --- |\n"
        # 确保按名称排序，让表格稳定
        for name in sorted(self.stats.keys()):
            s = self.stats[name]
            md += f"| {name} | {s.get('rows',0):,} | {s.get('stock_count',0)} | {s.get('date_range','-')} | {s.get('size_mb',0)} | {s.get('anomaly_count',0)} | {s.get('columns','-')} |\n"

        missing = self.diagnostics["missing_stocks"]
        if missing:
            md += f"\n\n## 🔍 数据缺失诊断 (发现 {len(missing)} 只标的重试 5 次后仍未获取到行情)\n\n"
            md += "| 代码 | 名称 | 状态 |\n| --- | --- | --- |\n"
            for item in missing[:50]:
                md += f"| {item['code']} | {item['code_name']} | ❌ 缺失 |\n"
        else:
            md += "\n\n✅ **完整性检查**：所有目标股票均已成功获取数据。"
        return md

    def save_report(self, path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.get_report_dict(), f, ensure_ascii=False, indent=2)
