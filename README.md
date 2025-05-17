## Available Commands

**1. exec `<plan.json>` [ `<name>` ]**

**Purpose:** Submit a workflow execution request.

**Details:**
- `<plan.json>`: Path to a logical plan file.
- `[<name>]` (optional): Custom execution name; defaults to the plan’s filename (without extension).

**Format auto-detection:**
- If the JSON text contains an `"operatorPositions"` field, it’s treated as exported JSON format and then it will be converted.
- Otherwise it’s loaded directly as converted JSON format.

**Example:**
```
> exec my_plan.json
```

---

**2. page `<operatorId>` `<size>` `<pageIndex>` [ `--export <dir>` ]**

**Purpose:** Fetch a page of results for a given operator.

**Positional arguments:**
- `<operatorId>`: Operator identifier string.
- `<size>`: Number of rows per page.
- `<pageIndex>`: 1-based page index.

**Optional** `--export <dir>` (`-e <dir>`): Directory where a JSON Lines file will be written.
- **File name:** `<operatorId>_<size>_<pageIndex>.jsonl`
- **Content:** Each line in the file is one JSON object from the result rows.

**Example:**
```
> page CSVFileScan-1 100 2 --export ./exports
```
This will:
1. Send a `ResultPaginationRequest` for page 2, size 100.
2. Upon receiving the `PaginatedResultEvent`, write each row as a JSON line into `./exports/CSVFileScan-1_100_2.jsonl`.

---

**3. kill**

**Purpose:** Abort the currently running workflow execution.

**Example:**
```
> kill
```
Sends a `WorkflowKillRequest` immediately.
