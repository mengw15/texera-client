import sys
import asyncio
import json
import os
from pathlib import Path

import websockets
from loguru import logger
import argparse

from converter import convertWorkflowContentToLogicalPlan

uri_base = "ws://localhost:8085/wsapi/workflow-websocket"
log_level = "INFO"
# mapping (operator, page) -> (export directory, requested page size)
export_requests = {}

# Configure loguru with colors and custom INFO level color
logger.remove()

logger.add(
    sys.stderr,
    level=log_level,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
           "<level>{level: <8}</level> | "
           "{message}",
    enqueue=True,
    colorize=True
)

cmd_parser = argparse.ArgumentParser(prog="")
subs = cmd_parser.add_subparsers(dest="command")

# exec subcommand: auto-detect different json format
def setup_exec_parser():
    p = subs.add_parser(
        "exec",
        help="exec <plan.json> [<name>]"
    )
    p.add_argument("plan", help="path to your plan.json")
    p.add_argument("name", nargs="?", help="execution name, defaults to plan filename")

setup_exec_parser()

# page subcommand
def setup_page_parser():
    p = subs.add_parser(
        "page",
        help="page <operatorId> <size> <pageIndex> [--export <dir>]"
    )
    p.add_argument("operator", help="Operator ID")
    p.add_argument("size", type=int, help="Page size (number of rows)")
    p.add_argument("page", type=int, help="Page index (integer), start from 1")
    p.add_argument(
        "--export", "-e", type=Path,
        help="Directory to export this page’s rows as JSON Lines; file '<operator>_<size>_<page>.jsonl' will be created"
    )

setup_page_parser()

# kill subcommand
def setup_kill_parser():
    subs.add_parser("kill", help="kill current workflow execution")

setup_kill_parser()

# -----------------------------------------------------------------------------
# individual send handlers for modularity
# -----------------------------------------------------------------------------
async def handle_kill(ws):
    await ws.send(json.dumps({"type": "WorkflowKillRequest"}))
    logger.info("Sent kill request")

async def handle_page(ws, ns):
    # schedule export if requested, store dir and requested size
    if ns.export:
        export_requests[(ns.operator, ns.page)] = (ns.export, ns.size)
        logger.info("Will export page {} of {} (size {}) to {}", ns.page, ns.operator, ns.size, ns.export)
    req = {
        "type": "ResultPaginationRequest",
        "requestID": f"req_{ns.operator}_{ns.size}_{ns.page}",
        "operatorID": ns.operator,
        "pageIndex": ns.page,
        "pageSize": ns.size
    }
    await ws.send(json.dumps(req))
    logger.info("Sent pagination request: operator={} size={} page={}", ns.operator, ns.size, ns.page)

async def handle_exec(ws, ns):
    plan_path = ns.plan
    if not os.path.isfile(plan_path):
        logger.error("Plan file not found: {}", plan_path)
        return
    name = ns.name or Path(plan_path).stem
    try:
        raw = Path(plan_path).read_text(encoding="utf-8")
        if '"operatorPositions"' in raw:
            logical_plan = convertWorkflowContentToLogicalPlan(raw)
            logger.debug("Auto-detected JSONA format")
        else:
            logical_plan = json.loads(raw)
            logger.debug("Auto-detected JSONB format")
    except Exception:
        logger.exception("Failed to read or convert plan")
        return
    payload = {
        "type": "WorkflowExecuteRequest",
        "executionName": name,
        "engineVersion": "3a1c33d6f",
        "logicalPlan": logical_plan,
        "workflowSettings": {"dataTransferBatchSize": 400},
        "emailNotificationEnabled": False
    }
    await ws.send(json.dumps(payload))
    logger.info("Sent execute request: name={} plan={}", name, plan_path)

# -----------------------------------------------------------------------------
# receiver: print events, use logger, handle JSONL export
# -----------------------------------------------------------------------------
async def receiver(ws):
    try:
        async for raw in ws:
            event = json.loads(raw)
            evt = event.get("type")

            if evt == "ExecutionDurationUpdateEvent":
                dur_ms = event.get("duration")
                is_running = event.get("isRunning")
                if not is_running:
                    logger.info("Final execution time: {:.2f}s", dur_ms / 1000)

            elif evt == "WorkflowStateEvent":
                logger.info("WorkflowState → {}", event['state'])

            elif evt == "WebResultUpdateEvent":
                updates = event.get("updates", {})
                for op_id, info in updates.items():
                    total = info.get("totalNumTuples")
                    logger.info("Result Operator Update: {} → totalNumTuples={}", op_id, total)

            elif evt == "PaginatedResultEvent":
                op = event['operatorID']
                pg = event['pageIndex']
                rows = event.get('table', [])
                logger.info("Result: operatorID={} size={} page={}", op, len(rows), pg)
                logger.info("Schema: {}", event['schema'])
                for row in rows:
                    logger.info("Row: {}", row)

                # export rows as JSON Lines if requested
                key = (op, pg)
                if key in export_requests:
                    out_dir, req_size = export_requests.pop(key)
                    out_dir.mkdir(parents=True, exist_ok=True)
                    filename = f"{op}_{req_size}_{pg}.jsonl"
                    path = out_dir / filename
                    with path.open("w", encoding="utf-8") as f:
                        for row in rows:
                            f.write(json.dumps(row, ensure_ascii=False))
                            f.write("\n")
                    logger.info("Exported {} rows as JSONL to {}", len(rows), path)

            # legacy handlers commented for optional enable
            # elif evt == "WorkerAssignmentUpdateEvent":
            #     logger.debug("WorkerAssignmentUpdate → Operator {operatorId} = {workerIds}", **event)
            # elif evt == "WorkflowErrorEvent":
            #     logger.error("ERROR → {fatalErrors}", **event)
            # else:
            #     logger.debug("Event {evt} → {event}", evt=evt, event=event)

    except websockets.ConnectionClosed:
        logger.warning("Connection closed, exiting receiver loop")

# -----------------------------------------------------------------------------
# sender: parse and dispatch commands
# -----------------------------------------------------------------------------
async def sender(ws):
    loop = asyncio.get_event_loop()
    HELP = """
Available commands:
  exec <plan.json> [<name>]
  page <operatorId> <size> <pageIdx> [--export <dir>]
  kill
"""
    print(HELP)
    global export_requests
    while True:
        line = await loop.run_in_executor(None, input, "> ")
        parts = line.strip().split()
        if not parts:
            continue
        try:
            ns = cmd_parser.parse_args(parts)
        except SystemExit:
            continue
        cmd = ns.command
        if cmd == "kill":
            await handle_kill(ws)
            continue
        if cmd == "page":
            await handle_page(ws, ns)
            continue
        if cmd == "exec":
            await handle_exec(ws, ns)
            continue
        logger.warning("Unknown command: {}", cmd)

# -----------------------------------------------------------------------------
# main connection loop (wid always "0")
# -----------------------------------------------------------------------------
async def connect_loop():
    uri = f"{uri_base}?wid=0"
    logger.info("Connecting to {}", uri)
    try:
        async with websockets.connect(uri) as ws:
            await asyncio.gather(receiver(ws), sender(ws))
    except Exception as e:
        logger.error("Connection failed: {}", e)

if __name__ == "__main__":
    try:
        asyncio.run(connect_loop())
    except KeyboardInterrupt:
        pass
