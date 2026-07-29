## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

When the user types `/graphify`, use the installed graphify skill or instructions before doing anything else.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- Dirty graphify-out/ files are expected after hooks or incremental updates; dirty graph files are not a reason to skip graphify. Only skip graphify if the task is about stale or incorrect graph output, or the user explicitly says not to use it.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).

## Work Summary (2026-07-29)

### Completed
- **Adjust Factor Validation** (`scripts/merge_and_push.py:103-221`): 4-rule DuckDB audit (A1/A2/B/C) with zero data copy
  - **A1** (invalid values): 0 anomalies
  - **A2** (surge >5x/<0.2x): 1 anomaly — sz.000630 @ 2015-10-23, ratio 5.05x (legitimate 10送40, no action needed)
  - **B** (monotonicity): 0 anomalies
  - **C** (adjusted price jump >21%): 11 remaining — all from 2006-2008, factor ratio ≤1.21, raw close change <21%, adjusted jump 21-38%. Confirmed as acceptable historical noise (~0.0001% of data)
  - Rule C went through 3 filtering iterations:
    1. Switched from comparing to `pctChg` (29,607 FPs) to adjusted price continuity check
    2. Added relisting filter (raw close change <21%) and extreme split filter (<5x), reduced 242→152
    3. Added factor self-jump filter (≤1.21), reduced 152→11
- **QC Report**: `utils/qc.py` renders adjust factor audit in `qc_summary.md`
- **Git**: All changes pushed to `main` (5 commits since init), runs via GitHub Actions

### Key Files
- `scripts/merge_and_push.py` — main pipeline + validation
- `utils/qc.py` — QC report rendering
