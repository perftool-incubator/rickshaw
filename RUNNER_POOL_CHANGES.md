# Runner Pool Support - Implementation Summary

## Changes Made to rickshaw/util/generate-ci-jobs.py

### 1. New Command Line Parameter
Added `--runner-pool` parameter (optional, defaults to empty string):
```python
--runner-pool "kmr-cloud-1"   # Use KMR cloud runners
--runner-pool "aws-cloud-1"   # Use AWS cloud runners
--runner-pool ""              # Don't specify pool (backward compatible)
```

### 2. New Function: build_runner_labels()
Creates the runner labels list dynamically based on runner type, tags, and pool.

**For self-hosted runners:**
- Always adds: `"self-hosted"`
- Conditionally adds pool identifier: `"kmr-cloud-1"` or `"aws-cloud-1"` (only if --runner-pool is provided)
- Adds runner tags: `"cpu-partitioning"`, `"remotehosts"`, etc.

**For GitHub-hosted runners:**
- Adds: `"ubuntu-latest"`

### 3. Modified Job Creation
Each job now includes a `runner_labels` field containing the list of runner labels.

## Backward Compatibility Guarantee

### Without --runner-pool (current behavior)
```bash
./util/generate-ci-jobs.py \
    --runner-type "self-hosted" \
    --runner-tags "cpu-partitioning,remotehosts" \
    --benchmark "cyclictest"
```

**Result:** Jobs created with `runner_labels: ["self-hosted", "cpu-partitioning", "remotehosts"]`

This is **backward compatible** because:
- `--runner-pool` defaults to `""`
- The code only adds pool label `if runner_pool:` (empty string is falsy)
- Existing crucible-ci workflows will work unchanged

### With --runner-pool (new behavior)
```bash
./util/generate-ci-jobs.py \
    --runner-type "self-hosted" \
    --runner-tags "cpu-partitioning,remotehosts" \
    --runner-pool "kmr-cloud-1" \
    --benchmark "cyclictest"
```

**Result:** Jobs created with `runner_labels: ["self-hosted", "kmr-cloud-1", "cpu-partitioning", "remotehosts"]`

## Job Output Format

### Before (job without runner_labels field)
```json
{
    "benchmark": "cyclictest",
    "enabled": true,
    "endpoint": "remotehosts"
}
```

### After (job with runner_labels field)
```json
{
    "benchmark": "cyclictest",
    "enabled": true,
    "endpoint": "remotehosts",
    "runner_labels": ["self-hosted", "cpu-partitioning", "remotehosts"]
}
```

Or with pool specified:
```json
{
    "benchmark": "cyclictest",
    "enabled": true,
    "endpoint": "remotehosts",
    "runner_labels": ["self-hosted", "kmr-cloud-1", "cpu-partitioning", "remotehosts"]
}
```

## Testing Backward Compatibility

### Scenario 1: Existing crucible-ci workflows (no changes)
- Crucible-ci does NOT pass `--runner-pool` parameter
- Script defaults to `--runner-pool ""`
- Jobs include `runner_labels` field but pool label is NOT included
- **Status: ✅ Works without any crucible-ci changes**

### Scenario 2: New crucible-ci workflows (with pool selection)
- Crucible-ci passes `--runner-pool "kmr-cloud-1"` or `--runner-pool "aws-cloud-1"`
- Jobs include pool-specific label in `runner_labels`
- Workflows use `runs-on: ${{ fromJSON(matrix.job.runner_labels) }}`
- **Status: ✅ Enables pool selection when crucible-ci is updated**

## Label Order

Labels are added in this order:
1. `"self-hosted"` (always first for self-hosted runners)
2. Pool identifier (e.g., `"kmr-cloud-1"`) - only if specified
3. Runner tags (e.g., `"cpu-partitioning"`, `"remotehosts"`)

This ensures consistent, predictable label ordering.
