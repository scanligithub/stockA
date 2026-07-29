import pandas as pd
import json
import os

class QualityControl:
    def __init__(self):
        self.report = {"errors": [], "stats": {}}

    def check_dataframe(self, df, name, critical_cols=[], file_path=None):
        """
        深度质检：包含字段清单、文件大小、异常统计等
        """
        stats = {
            "total_rows": len(df),
            "columns": list(df.columns),  # 保存字段清单
            "anomalies": {},
            "anomaly_count": 0,
            "file_size_mb": 0.0
        }
        
        if df.empty:
            self.report["errors"].append(f"{name} is empty!")
            return

        # 1. 获取文件大小 (MB)
        if file_path and os.path.exists(file_path):
            stats["file_size_mb"] = round(os.path.getsize(file_path) / (1024 * 1024), 2)

        # 2. 时间范围统计
        if "date" in df.columns:
            stats["start_date"] = str(df["date"].min())
            stats["end_date"] = str(df["date"].max())

        # 3. 个股/板块数量统计
        if "code" in df.columns:
            stats["unique_codes"] = int(df["code"].nunique())
            
        # 4. 异常检测与计数
        anomaly_details = {}
        if "high" in df.columns and "low" in df.columns:
            # 过滤正常范围，只计真正异常
            mask = (df['high'] > 0) & (df['low'] > 0) & (df['high'] < df['low'])
            err = int(df[mask].shape[0])
            if err > 0: anomaly_details["high_lt_low"] = err
            
        if "volume" in df.columns:
            err = int(df[df['volume'] < 0].shape[0])
            if err > 0: anomaly_details["neg_volume"] = err

        for col in critical_cols:
            if col in df.columns:
                nulls = int(df[col].isnull().sum())
                if nulls > 0: anomaly_details[f"null_{col}"] = nulls

        stats["anomalies"] = anomaly_details
        stats["anomaly_count"] = sum(anomaly_details.values())
        stats["anomaly_types"] = list(anomaly_details.keys())

        self.report["stats"][name] = stats

    def save_report(self, path):
        dir_name = os.path.dirname(path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.report, f, indent=2, ensure_ascii=False)
            
    def get_summary_md(self):
        md = "# 📊 数据质量深度质检报告\n\n"
        if self.report["errors"]:
            md += "## ❌ 严重错误\n"
            for e in self.report["errors"]: md += f"- {e}\n"
        
        md += "## 📈 数据产物概览\n"
        # 增加了“字段清单”列
        md += "| 文件名 | 行数 | 标的数量 | 时间范围 | 大小(MB) | 异常数 | 字段清单 |\n"
        md += "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n"
        
        for name, stat in self.report["stats"].items():
            range_str = f"{stat.get('start_date','-')}~{stat.get('end_date','-')}"
            # 字段清单较长，用逗号连接
            fields_str = ", ".join(stat.get("columns", []))
            md += f"| {name} | {stat['total_rows']:,} | {stat.get('unique_codes','-')} | {range_str} | {stat['file_size_mb']} | {stat['anomaly_count']} | {fields_str} |\n"
        
        # 🔍 复权因子深度审计
        if "adjust_factor_audit" in self.report and self.report["adjust_factor_audit"]:
            aud = self.report["adjust_factor_audit"]
            md += "\n## 🔍 复权因子深度审计\n\n"
            md += "| 规则 | 说明 | 异常数 |\n"
            md += "| :--- | :--- | ---: |\n"
            md += f"| A1 | 物理边界 (<=0/NaN/Inf) | {aud.get('A1_invalid_values', 'N/A')} |\n"
            md += f"| A2 | 单日暴跳 (>5x/<0.2x) | {aud.get('A2_surge_events', 'N/A')} |\n"
            md += f"| B  | 单调性违规 (非递减) | {aud.get('B_monotonicity_violations', 'N/A')} |\n"
            md += f"| C  | 除权日复权价跳变 (>21%) | {aud.get('C_ex_div_price_jumps', 'N/A')} |\n"
            total = sum(v for v in aud.values() if isinstance(v, (int, float)))
            md += f"\n**复权因子异常总计: {int(total)}**\n"
        
        return md
