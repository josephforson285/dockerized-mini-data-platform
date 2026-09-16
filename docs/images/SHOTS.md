# Screenshots to capture

Save as PNG in this folder, using the exact filenames below — the docs
reference them by name. Crop to the relevant panel; no full desktop, no
personal browser tabs. Check each one for visible credentials before saving.

The stack must be running with data loaded:

```bash
make nuke && make up
.venv/bin/python -m data_generator.generate \
  --batch-id demo --rows 2000 --seed 42 --corrupt 60 --duplicates 40 --upload
```

Then trigger `sales_pipeline` once and let it finish.

---

## CI/CD — the graded core

### 1. `ci-pipeline-green.png`
**Where:** GitHub → Actions → newest run on `main`
**Capture:** the jobs list showing all six green — lint, unit tests, dag
integrity, end-to-end, publish image, deploy to test environment
**Goes in:** README, CI/CD section

### 2. `ci-deploy-published-image.png`
**Where:** that run → `deploy to test environment` → expand *Deploy the
published image*
**Capture:** the lines showing `PUBLISHED_IMAGE: ghcr.io/...:sha-<commit>` and
`Status: Downloaded newer image`
**Goes in:** README, CI/CD section — this is the proof that the deploy runs the
published artifact rather than a rebuild

### 3. `branch-protection.png`
**Where:** GitHub → Settings → Branches → rule for `main`
**Capture:** require a PR, the four required status checks, "do not allow
bypassing", force pushes disabled
**Goes in:** README, CI/CD section

### 4. `pr-checks.png`
**Where:** any merged PR → Checks tab (or the merged conversation view)
**Capture:** the four checks green and the auto-merge note
**Goes in:** `docs/brief-compliance.md`, CI/CD row

### 5. `branch-network.png`
**Where:** GitHub → Insights → Network
**Capture:** the graph showing branches splitting from `main` and merging back
**Goes in:** README, near the contributing section

---

## The platform

### 6. `airflow-dag-success.png`
**Where:** Airflow → `sales_pipeline` → Graph, after a successful run
**Capture:** all four tasks green — discover, ingest, verify_load, summarise
**Goes in:** README, "How the pipeline works"

### 7. `airflow-quality-gate-failed.png`
**Where:** trigger a bad batch, then the Graph of that run

```bash
.venv/bin/python -m data_generator.generate \
  --batch-id broken --rows 2000 --seed 9 --corrupt 1800 --duplicates 0 --upload
```

Trigger with `{"batch_id": "broken"}`.
**Capture:** `ingest` red, `verify_load` and `summarise` upstream_failed.
Ideally include the log line `1800/2000 rows rejected (90.0%); limit is 50.0%`
**Goes in:** README, design notes — the single best evidence of data-quality
gating

### 8. `metabase-dashboard.png`
**Where:** Metabase → Our analytics → Sales Overview
**Capture:** the whole dashboard — four KPI tiles, the daily trend, the
breakdowns, rejected rows by reason
**Goes in:** README, Dashboard section

### 9. `minio-bucket.png`
**Where:** MinIO console → Object Browser → `raw` → `sales/`
**Capture:** the batch CSVs listed
**Goes in:** README, "How the pipeline works"

### 10. `postgres-reject-reasons.png`
**Where:** terminal or SQL client

```sql
SELECT reject_reason, count(*) FROM rejected_sales GROUP BY 1 ORDER BY 2 DESC;
```

**Capture:** the result — duplicate_key, missing_required_field, and the rest
**Goes in:** README, design notes, next to the quarantine point

---

## Before committing

- No passwords, tokens or session cookies visible. `.env` values appear in the
  MinIO and Metabase login screens — do not capture those.
- Keep each file under ~500 KB; PNG at normal window size is fine.
- Dark or light theme, but be consistent.
