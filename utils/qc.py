import pandas as pd
import json
import os

class QualityControl:
    def __init__(self):
        self.report = {"errors": [], "stats": {}}

    def check_dataframe(self, df, name, critical_cols=[]):
        stats = {
            "total_rows": len(df),
            "columns": list(df.columns),
            "anomalies": {}
        }
        if df.empty:
            self.report["errors"].append(f"{name} is empty!")
            return

        # === 新增：时间范围统计 ===
        if "date" in df.columns:
            stats["start_date"] = str(df["date"].min())
            stats["end_date"] = str(df["date"].max())

        # === 个股/板块数量统计 ===
        if "code" in df.columns:
            stats["unique_codes"] = int(df["code"].nunique())
            
        # 异常检测
        if "high" in df.columns and "low" in df.columns:
            # 过滤掉高低价为0的情况（可能是停牌）
            mask = (df['high'] > 0) & (df['low'] > 0) & (df['high'] < df['low'])
            err = df[mask].shape[0]
            if err > 0: stats["anomalies"]["high_lt_low"] = int(err)
            
        if "volume" in df.columns:
            err = df[df['volume'] < 0].shape[0]
            if err > 0: stats["anomalies"]["neg_volume"] = int(err)

        for col in critical_cols:
            if col in df.columns:
                nulls = df[col].isnull().sum()
                if nulls > 0: stats["anomalies"][f"null_{col}"] = int(nulls)

        self.report["stats"][name] = stats

    def save_report(self, path):
        dir_name = os.path.dirname(path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
            
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.report, f, indent=2)
            
    def get_summary_md(self):
        md = "# 📊 Data Quality Report\n\n"
        if self.report["errors"]:
            md += "## ❌ Critical Errors\n"
            for e in self.report["errors"]: md += f"- {e}\n"
        md += "## 📈 Statistics\n"
        for name, stat in self.report["stats"].items():
            md += f"### {name}\n"
            md += f"- **Rows**: {stat['total_rows']:,}\n"
            if "unique_codes" in stat: 
                md += f"- **Codes**: {stat['unique_codes']:,}\n"
            if "start_date" in stat:
                md += f"- **Range**: {stat['start_date']} ~ {stat['end_date']}\n"
            
            if stat['anomalies']:
                md += "- **Anomalies**:\n"
                for k, v in stat['anomalies'].items(): md += f"  - {k}: {v}\n"
            else: md += "- ✅ No anomalies detected.\n"
        return md
