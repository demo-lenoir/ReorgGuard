#!/usr/bin/env python3
"""One-command localhost Anvil/PostgreSQL/ReorgGuard correctness demonstration."""
import hashlib
import json
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(*args, env=None):
    return subprocess.check_output(args, cwd=ROOT, env=env, text=True, stderr=subprocess.STDOUT).strip()


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http(url, data=None, timeout=3):
    payload = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"} if payload else {})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        body = response.read()
        return json.loads(body) if body and response.headers.get_content_type() == "application/json" else body.decode()


def rpc(url, method, params=None):
    result = http(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []})
    if "error" in result:
        raise RuntimeError(f"RPC {method} failed: {result['error'].get('code')}")
    return result["result"]


def wait(label, predicate, seconds=20):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, urllib.error.URLError, ValueError, KeyError) as exc:
            last = type(exc).__name__
        time.sleep(0.1)
    raise RuntimeError(f"timeout waiting for {label}: {last}")


def control(base, transport, state):
    req = urllib.request.Request(f"{base}/control/{transport}/{state}", data=b"", method="POST")
    with urllib.request.urlopen(req, timeout=3) as response:
        assert response.status == 204


def main():
    started = time.monotonic()
    container = None
    children = []
    with tempfile.TemporaryDirectory(prefix="reorgguard-demo-") as tmp:
        logs = []
        def launch(name, args, env=None):
            path = pathlib.Path(tmp) / f"{name}.log"
            handle = path.open("w")
            logs.append(handle)
            child = subprocess.Popen(args, cwd=ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT)
            children.append(child)
            return child
        try:
            run("go", "build", "-o", f"{tmp}/reorgguard", "./cmd/reorgguard")
            run("go", "build", "-o", f"{tmp}/demo-proxy", "./cmd/demo-proxy")
            run("forge", "build")
            container = run("docker", "run", "-d", "--rm", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-p", "127.0.0.1::5432", "postgres:18.6")
            wait("PostgreSQL", lambda: subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres", "-d", "postgres"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0)
            pg_port = run("docker", "port", container, "5432/tcp").split(":")[-1]
            anvil_port, proxy_port, fallback_port, api_port = port(), port(), port(), port()
            anvil = f"http://127.0.0.1:{anvil_port}"
            proxy = f"http://127.0.0.1:{proxy_port}"
            fallback = f"http://127.0.0.1:{fallback_port}"
            api = f"http://127.0.0.1:{api_port}"
            launch("anvil", ["anvil", "--host", "127.0.0.1", "--port", str(anvil_port), "--chain-id", "31337", "--timestamp", "1700000000", "--silent"])
            wait("Anvil", lambda: rpc(anvil, "eth_chainId") == "0x7a69")
            launch("proxy", [f"{tmp}/demo-proxy"], {**os.environ, "DEMO_UPSTREAM": anvil, "DEMO_LISTEN": f"127.0.0.1:{proxy_port}"})
            wait("fault proxy", lambda: http(proxy + "/state"))
            launch("fallback-proxy", [f"{tmp}/demo-proxy"], {**os.environ, "DEMO_UPSTREAM": anvil, "DEMO_LISTEN": f"127.0.0.1:{fallback_port}"})
            wait("fallback fault proxy", lambda: http(fallback + "/state"))
            genesis = rpc(anvil, "eth_getBlockByNumber", ["0x0", False])["hash"]
            sender = rpc(anvil, "eth_accounts")[0]
            artifact = json.loads((ROOT / "out/EventEmitter.sol/EventEmitter.json").read_text())
            bytecode = artifact["bytecode"]["object"]
            def next_timestamp():
                next_height = int(rpc(anvil, "eth_blockNumber"), 16) + 1
                rpc(anvil, "evm_setNextBlockTimestamp", [1700000000 + next_height])
            next_timestamp()
            receipt = json.loads(run("cast", "send", "--unlocked", "--from", sender, "--rpc-url", anvil, "--create", bytecode, "--json"))
            contract = receipt["contractAddress"]
            def emit(value):
                next_timestamp()
                json.loads(run("cast", "send", "--unlocked", "--from", sender, "--rpc-url", anvil, contract, "emitEvent(uint256)", str(value), "--json"))
                return int(rpc(anvil, "eth_blockNumber"), 16)
            historical = emit(1)
            env = {**os.environ,
                "REORGGUARD_RPC_HTTP_URL": proxy,
                "REORGGUARD_RPC_FALLBACK_URLS": fallback,
                "REORGGUARD_RPC_WS_URL": proxy.replace("http://", "ws://"),
                "REORGGUARD_DATABASE_URL": f"postgres://postgres@127.0.0.1:{pg_port}/postgres?sslmode=disable",
                "REORGGUARD_CHAIN_ID": "31337", "REORGGUARD_GENESIS_HASH": genesis,
                "REORGGUARD_START_BLOCK": "1", "REORGGUARD_ADDRESS": contract,
                "REORGGUARD_MAX_REORG_DEPTH": "8", "REORGGUARD_SYNC_MODE": "live",
                "REORGGUARD_POLL_INTERVAL": "500ms", "REORGGUARD_API_ADDR": f"127.0.0.1:{api_port}",
                "REORGGUARD_TRACING": "stdout"}
            service = launch("reorgguard-1", [f"{tmp}/reorgguard"], env)
            def status(): return http(api + "/v1/status")
            def at(height, branch_hash=None):
                def check():
                    x = status()
                    return x if x["indexed_head"] == height and x["ready"] and (branch_hash is None or x["canonical_head_hash"].lower() == branch_hash.lower()) else None
                return wait(f"checkpoint {height}", check, 30)
            at(historical)
            assert http(api + "/health/ready")["ready"]
            assert status()["lag_blocks"] == 0
            print("PASS historical backfill and readiness")
            live_height = emit(2)
            at(live_height)
            print("PASS live event")
            control(proxy, "ws", "off")
            wait("WS degraded", lambda: status()["websocket_state"] == "degraded")
            control(proxy, "http", "off")
            control(fallback, "http", "off")
            missed_height = emit(3)
            def unready():
                try:
                    http(api + "/health/ready")
                    return False
                except urllib.error.HTTPError as exc:
                    return exc.code == 503
            wait("both transports unavailable", unready)
            assert status()["indexed_head"] == live_height
            control(proxy, "http", "on")
            control(fallback, "http", "on")
            at(missed_height)
            control(proxy, "ws", "on")
            wait("WS reconnected", lambda: status()["websocket_state"] == "connected")
            wait("primary recovered", lambda: status()["active_rpc_endpoint"] == "primary", 15)
            print("PASS WS loss, missing-block gap, HTTP catch-up and reconnect")
            snapshot = rpc(anvil, "evm_snapshot")
            branch_height = emit(4)
            old_hash = rpc(anvil, "eth_getBlockByNumber", [hex(branch_height), False])["hash"]
            at(branch_height, old_hash)
            # Anvil's evm_revert drops orphaned hash lookups. The demo proxy
            # retains the previously observed immutable header/log set, as an
            # archival RPC normally would for checkpoint compatibility checks.
            old_block = rpc(anvil, "eth_getBlockByHash", [old_hash, False])
            old_logs = rpc(anvil, "eth_getLogs", [{"blockHash": old_hash, "address": contract}])
            http(proxy + "/control/archive", {"hash": old_hash, "block": old_block, "logs": old_logs})
            assert rpc(anvil, "evm_revert", [snapshot]) is True
            snapshot_before_b = rpc(anvil, "evm_snapshot")
            new_height = emit(5)
            assert new_height == branch_height
            new_hash = rpc(anvil, "eth_getBlockByNumber", [hex(new_height), False])["hash"]
            assert old_hash != new_hash
            at(new_height, new_hash)
            page = http(api + "/v1/logs?limit=100")
            assert len(page["items"]) == 4 and all(x["block_hash"].lower() != old_hash.lower() for x in page["items"])
            assert status()["last_reorg"]["depth"] == 1
            print("PASS depth-1 reorg and orphaned event filtering")
            # Archive B before reverting Anvil again. Replaying the original
            # transaction at the pinned timestamp reproduces A's exact hash.
            branch_b_block = rpc(anvil, "eth_getBlockByHash", [new_hash, False])
            branch_b_logs = rpc(anvil, "eth_getLogs", [{"blockHash": new_hash, "address": contract}])
            http(proxy + "/control/archive", {"hash": new_hash, "block": branch_b_block, "logs": branch_b_logs})
            assert rpc(anvil, "evm_revert", [snapshot_before_b]) is True
            assert emit(4) == branch_height
            returned_hash = rpc(anvil, "eth_getBlockByNumber", [hex(branch_height), False])["hash"]
            assert returned_hash == old_hash
            at(branch_height, old_hash)
            page = http(api + "/v1/logs?limit=100")
            assert len(page["items"]) == 4 and all(x["block_hash"].lower() != new_hash.lower() for x in page["items"])
            print("PASS A -> B -> A exact re-canonicalization")
            service.kill(); service.wait(timeout=5)
            service = launch("reorgguard-2", [f"{tmp}/reorgguard"], env)
            at(branch_height, old_hash)
            print("PASS SIGKILL and durable checkpoint restart")
            control(proxy, "http", "off")
            failover_height = emit(6)
            wait("guarded fallback", lambda: (x if (x := status())["indexed_head"] == failover_height and x["active_rpc_endpoint"] == "fallback_1" and x["ready"] else None), 30)
            control(proxy, "http", "on")
            print("PASS primary failure and validated fallback continuation")
            def query(command):
                return run("psql", "-h", "127.0.0.1", "-p", pg_port, "-U", "postgres", "-d", "postgres", "-At", "-c", command)
            checkpoint = query("SELECT checkpoint_number || ':' || encode(checkpoint_hash,'hex') FROM sync_state")
            block_count = int(query("SELECT count(*) FROM blocks WHERE canonical"))
            log_count = int(query("SELECT count(*) FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash WHERE b.canonical"))
            orphan_count = int(query("SELECT count(*) FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash WHERE NOT b.canonical"))
            orphan_blocks = int(query("SELECT count(*) FROM blocks WHERE NOT canonical"))
            duplicates = int(query("SELECT count(*)-count(DISTINCT (l.block_hash,l.log_index)) FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash WHERE b.canonical"))
            invalid = int(query("SELECT count(*) FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash WHERE b.canonical AND (NOT b.canonical OR b.number<>l.block_number)"))
            checkpoint_valid = query("SELECT count(*) FROM sync_state st JOIN blocks b ON b.chain_id=st.chain_id AND b.number=st.checkpoint_number AND b.hash=st.checkpoint_hash WHERE b.canonical")
            canonical_lines = query("SELECT 'B|'||b.number||'|'||encode(b.hash,'hex')||'|'||encode(b.parent_hash,'hex')||'|'||extract(epoch from b.block_time)::text FROM blocks b WHERE b.canonical UNION ALL SELECT 'L|'||l.block_number||'|'||encode(l.block_hash,'hex')||'|'||encode(l.tx_hash,'hex')||'|'||l.tx_index||'|'||l.log_index||'|'||encode(l.address,'hex')||'|'||array_to_string(ARRAY(SELECT encode(topic,'hex') FROM unnest(l.topics) AS topic),',')||'|'||encode(l.data,'hex') FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash WHERE b.canonical ORDER BY 1")
            checksum = hashlib.sha256((canonical_lines + "\n").encode()).hexdigest()
            final = status()
            assert checkpoint == f"{failover_height}:{final['canonical_head_hash'][2:].lower()}"
            assert final["lag_blocks"] == 0
            assert block_count == failover_height + 1 and log_count == 5 and orphan_count == 1 and orphan_blocks == 1 and duplicates == 0 and invalid == 0 and checkpoint_valid == "1"
            assert len(http(api + "/v1/logs?limit=100")["items"]) == log_count
            assert http(api + "/health/ready")["ready"]
            metrics = http(api + "/metrics")
            for name in ("reorgguard_remote_head_block", "reorgguard_reorgs_total", "reorgguard_rpc_failovers_total", "reorgguard_logs_processed_total"):
                assert name in metrics, name
            service.terminate()
            assert service.wait(timeout=7) == 0
            first_log = (pathlib.Path(tmp) / "reorgguard-1.log").read_text(errors="replace")
            second_log = (pathlib.Path(tmp) / "reorgguard-2.log").read_text(errors="replace")
            assert "canonical reorg committed" in first_log
            assert "HTTP RPC failover" in second_log
            assert "provider.sweep" in second_log and "db.checkpoint_update" in second_log
            summary = {"result": "PASS", "duration_seconds": round(time.monotonic() - started, 2), "checkpoint": checkpoint, "canonical_blocks": block_count, "canonical_logs": log_count, "orphan_blocks": orphan_blocks, "orphan_logs": orphan_count, "checksum": checksum}
            print(json.dumps(summary, sort_keys=True))
        except Exception:
            for handle in logs: handle.flush()
            for name in ("anvil", "proxy", "fallback-proxy", "reorgguard-1", "reorgguard-2"):
                path = pathlib.Path(tmp) / f"{name}.log"
                if path.exists():
                    print(f"--- {name} tail ---\n" + "\n".join(path.read_text(errors="replace").splitlines()[-15:]), file=sys.stderr)
            raise
        finally:
            for child in reversed(children):
                if child.poll() is None:
                    child.terminate()
                    try: child.wait(timeout=5)
                    except subprocess.TimeoutExpired: child.kill(); child.wait(timeout=5)
            for handle in logs: handle.close()
            if container:
                subprocess.run(["docker", "stop", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)


if __name__ == "__main__":
    main()
