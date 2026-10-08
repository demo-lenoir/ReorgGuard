#!/usr/bin/env python3
"""Deterministic local HTTP RPC + real PostgreSQL correctness-gated benchmark."""
import argparse
import hashlib
import json
import os
import pathlib
import platform
import resource
import statistics
import subprocess
import tempfile
import threading
import time
from decimal import Decimal, ROUND_HALF_EVEN
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from artifact_identity import source_digest, canonical_target

ROOT = pathlib.Path(__file__).resolve().parents[1]
ADDRESS = '0x' + '23' * 20
TOPIC = '0x' + '45' * 32
CHAIN_ID = 31337
VERSION = 3


def rounded_measurement(nanoseconds, count=None):
    """Display duration to 4 places or an exact-nanosecond rate to 2."""
    if nanoseconds <= 0:
        raise ValueError('nonpositive benchmark duration')
    if count is None:
        value, quantum = Decimal(nanoseconds) / Decimal(1000000000), Decimal('0.0001')
    else:
        value, quantum = Decimal(count) * Decimal(1000000000) / Decimal(nanoseconds), Decimal('0.01')
    return float(value.quantize(quantum, rounding=ROUND_HALF_EVEN))


def run_identity(source_commit, index, scenario, workers, height):
    return hashlib.sha256(f'{source_commit}:{index}:{scenario}:{workers}:{height}'.encode()).hexdigest()


def dataset_identity(scenario, height, reference):
    return hashlib.sha256(f'reorgguard-benchmark-v1:{scenario}:{height}:{reference}'.encode()).hexdigest()


def digest(s):
    return '0x' + hashlib.sha256(s.encode()).hexdigest()


def dataset(name, height):
    blocks, logs = [], []
    for n in range(height + 1):
        bh = digest(f'reorgguard-benchmark-v1-block-{n}')
        parent = '0x' + '00' * 32 if n == 0 else blocks[-1]['hash']
        blocks.append({'number': hex(n), 'hash': bh, 'parentHash': parent,
                       'timestamp': hex(1700000000 + n)})
        if n == 0:
            continue
        count = {'sparse': int(n % 12 == 0), 'dense': 3,
                 'mixed': (n * 7 + n // 9) % 4}[name]
        for j in range(count):
            payload = bytes([(n + j) % 256]) * {'sparse': 32, 'dense': 96, 'mixed': 48}[name]
            logs.append({'blockHash': bh, 'blockNumber': hex(n),
                         'transactionHash': digest(f'reorgguard-benchmark-v1-tx-{n}-{j}'),
                         'transactionIndex': hex(j), 'logIndex': hex(j),
                         'address': ADDRESS, 'topics': [TOPIC], 'data': '0x' + payload.hex(),
                         'removed': False})
    return blocks, logs


def expected_lines(blocks, logs):
    lines = []
    for b in blocks:
        lines.append(f"B|{int(b['number'],16)}|{b['hash'][2:]}|{b['parentHash'][2:]}|{int(b['timestamp'],16)}")
    for l in logs:
        lines.append(f"L|{int(l['blockNumber'],16)}|{l['blockHash'][2:]}|{l['transactionHash'][2:]}|{int(l['transactionIndex'],16)}|{int(l['logIndex'],16)}|{l['address'][2:]}|{l['topics'][0][2:]}|{l['data'][2:]}")
    return sorted(lines)


def checksum(lines):
    return hashlib.sha256(('\n'.join(lines) + '\n').encode()).hexdigest()


def validate(expected, actual):
    for key in ('checkpoint', 'head_hash', 'blocks', 'logs', 'duplicates',
                'parent_errors', 'orphan_blocks', 'orphan_logs', 'checksum'):
        if actual[key] != expected[key]:
            raise ValueError(f'correctness failure: {key}: expected {expected[key]}, got {actual[key]}')


class Fixture(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, blocks, logs, host='127.0.0.1', delay=0):
        super().__init__((host, 0), Handler)
        self.blocks = blocks
        self.logs = logs
        self.latencies = []
        self.lock = threading.Lock()
        self.requests = 0
        self.delay = delay
        self.active = 0
        self.max_active = 0
        self.range_sizes = []
        self.duplicate_deliveries = 0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        started = time.perf_counter()
        with self.server.lock:
            self.server.active += 1
            self.server.max_active = max(self.server.max_active, self.server.active)
        try:
            if self.server.delay:
                time.sleep(self.server.delay)
            length = int(self.headers['Content-Length'])
            if length > 1 << 20:
                raise ValueError('request too large')
            req = json.loads(self.rfile.read(length))
            method, params = req['method'], req['params']
            if method == 'eth_chainId':
                value = hex(CHAIN_ID)
            elif method == 'eth_blockNumber':
                value = hex(len(self.server.blocks) - 1)
            elif method == 'eth_getBlockByNumber':
                n = int(params[0], 16)
                value = self.server.blocks[n] if n < len(self.server.blocks) else None
            elif method == 'eth_getBlockByHash':
                value = next((b for b in self.server.blocks if b['hash'] == params[0]), None)
            elif method == 'eth_getLogs':
                f = params[0]
                if 'blockHash' in f:
                    value = [l for l in self.server.logs if l['blockHash'] == f['blockHash']]
                else:
                    lo, hi = int(f['fromBlock'], 16), int(f['toBlock'], 16)
                    with self.server.lock:
                        self.server.range_sizes.append(hi - lo + 1)
                    value = [l for l in self.server.logs if lo <= int(l['blockNumber'], 16) <= hi]
                # Repeat exact delivery for some blocks. The canonical projection
                # must still have one log per immutable identity.
                duplicates = [l for l in value if int(l['blockNumber'], 16) % 17 == 0]
                with self.server.lock:
                    self.server.duplicate_deliveries += len(duplicates)
                value = value + duplicates
            else:
                raise ValueError('unsupported RPC method')
            payload = json.dumps({'jsonrpc': '2.0', 'id': req['id'], 'result': value}, separators=(',', ':')).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (ValueError, KeyError, IndexError):
            self.send_error(400)
        finally:
            with self.server.lock:
                self.server.requests += 1
                self.server.active -= 1
                self.server.latencies.append(time.perf_counter() - started)


def run(*args, env=None):
    try:
        return subprocess.check_output(args, cwd=ROOT, env=env, stderr=subprocess.STDOUT, text=True).strip()
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f'command {args[0]} failed: {exc.output[-500:]}') from exc


def percentile(values, p):
    ordered = sorted(values)
    return round(ordered[max(0, (len(ordered) * p + 99) // 100 - 1)] * 1000, 4)


def inspect_db(port, db):
    def query(sql):
        return run('psql', '-h', '127.0.0.1', '-p', str(port), '-U', 'postgres', '-d', db, '-At', '-c', sql)
    head = query("SELECT checkpoint_number || '|' || encode(checkpoint_hash,'hex') FROM sync_state")
    lines = query("""SELECT line FROM (
        SELECT 'B|'||b.number||'|'||encode(b.hash,'hex')||'|'||encode(b.parent_hash,'hex')||'|'||extract(epoch from b.block_time)::bigint AS line FROM blocks b WHERE b.canonical
        UNION ALL
        SELECT 'L|'||l.block_number||'|'||encode(l.block_hash,'hex')||'|'||encode(l.tx_hash,'hex')||'|'||l.tx_index||'|'||l.log_index||'|'||encode(l.address,'hex')||'|'||array_to_string(ARRAY(SELECT encode(topic,'hex') FROM unnest(l.topics) AS topic),',')||'|'||encode(l.data,'hex') FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash WHERE b.canonical
    ) ordered ORDER BY line COLLATE "C";""").splitlines()
    counts = query("""SELECT
      (SELECT count(*) FROM blocks WHERE canonical),
      (SELECT count(*) FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash WHERE b.canonical),
      (SELECT count(*)-count(DISTINCT (l.block_hash,l.log_index)) FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash WHERE b.canonical),
      (SELECT count(*) FROM blocks b LEFT JOIN blocks p ON p.chain_id=b.chain_id AND p.hash=b.parent_hash AND p.number=b.number-1 AND p.canonical WHERE b.canonical AND b.number>0 AND p.hash IS NULL),
      (SELECT count(*) FROM blocks WHERE NOT canonical),
      (SELECT count(*) FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash WHERE NOT b.canonical)""")
    numbers = list(map(int, counts.split('|')))
    return dict(checkpoint=int(head.split('|')[0]), head_hash=head.split('|')[1],
                blocks=numbers[0], logs=numbers[1], duplicates=numbers[2],
                parent_errors=numbers[3], orphan_blocks=numbers[4], orphan_logs=numbers[5],
                checksum=checksum(lines), _lines=lines)


def machine():
    cpu = platform.processor() or platform.machine()
    if platform.system() == 'Darwin':
        cpu = run('sysctl', '-n', 'machdep.cpu.brand_string') if platform.machine() == 'x86_64' else run('sysctl', '-n', 'hw.model')
        ram = int(run('sysctl', '-n', 'hw.memsize'))
    else:
        ram = int(os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE'))
        cpu_info = pathlib.Path('/proc/cpuinfo')
        if cpu_info.exists():
            cpu = next((line.split(':', 1)[1].strip() for line in cpu_info.read_text().splitlines() if line.startswith('model name')), cpu)
    docker_limits = {'memory_bytes': int(run('docker', 'info', '--format', '{{.MemTotal}}')),
                     'logical_cpus': int(run('docker', 'info', '--format', '{{.NCPU}}')),
                     'storage_driver': run('docker', 'info', '--format', '{{.Driver}}')}
    return {'os': platform.system(), 'os_release': platform.release(),
            'arch': platform.machine(), 'cpu': cpu, 'logical_cpus': os.cpu_count(),
            'ram_bytes': ram, 'go': run('go', 'version'), 'docker_server': run('docker', 'version', '--format', '{{.Server.Version}}'),
            'postgres_image': 'postgres:18.6', 'storage': 'temporary PostgreSQL container writable layer, ' + docker_limits['storage_driver'],
            'container_limits': docker_limits}


def benchmark(smoke, output):
    height = 96 if smoke else 1024
    plans = [('mixed', 1, 0), ('mixed', 4, 8)] if smoke else [(density, workers, 0 if workers == 1 else workers * 2)
        for density in ('sparse', 'dense', 'mixed') for workers in (1, 4, 8)] + [('mixed', 4, 8), ('mixed', 4, 8)]
    metadata = machine()
    source_commit = run('git', 'rev-parse', 'HEAD')
    if not smoke and source_digest(source_commit) != source_digest():
        raise ValueError('benchmark application source must be committed before measurement')
    target = canonical_target(platform.system(), platform.machine())
    result = {'harness_version': VERSION, 'source_commit': source_commit,
              'harness_sha256': hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
              'app_source_sha256': source_digest(), 'binary_target': target,
              'dataset_version': 'reorgguard-benchmark-v1',
              'environment': metadata, 'height': height, 'config': {
                  'initial_range': 64, 'min_range': 1, 'max_range': 1024,
                  'db_pool_max_conns': 8, 'rpc_fixture_delay_ms': 3 if smoke else 0,
                  'adaptive_growth_after': 3, 'max_logs_per_range': 10000}, 'runs': []}
    with tempfile.TemporaryDirectory(prefix='reorgguard-benchmark-') as tmp:
        binary = pathlib.Path(tmp) / 'reorgguard'
        subprocess.run(['go', 'build', '-buildvcs=false', '-trimpath', '-ldflags=-s -w',
                        '-o', str(binary), './cmd/reorgguard'], cwd=ROOT,
                       env={**os.environ, 'CGO_ENABLED': '0'}, check=True)
        result['binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
        container = run('docker', 'run', '-d', '--rm', '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', '-p', '127.0.0.1::5432', 'postgres:18.6')
        try:
            port = int(run('docker', 'port', container, '5432/tcp').split(':')[-1])
            for _ in range(100):
                log_output = subprocess.run(['docker', 'logs', container], capture_output=True, text=True)
                initialized = 'PostgreSQL init process complete; ready for start up.' in (log_output.stdout + log_output.stderr)
                ready = subprocess.run(['psql', '-h', '127.0.0.1', '-p', str(port), '-U', 'postgres', '-d', 'postgres', '-At', '-c', 'SELECT 1'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if initialized and ready.returncode == 0:
                    break
                time.sleep(0.1)
            else:
                raise RuntimeError('PostgreSQL did not become ready')
            result['environment']['postgres_version'] = run('psql', '-h', '127.0.0.1', '-p', str(port), '-U', 'postgres', '-d', 'postgres', '-At', '-c', 'SHOW server_version')
            result['environment']['indexes'] = []
            for idx, (density, workers, inflight) in enumerate(plans):
                blocks, logs = dataset(density, height)
                reference_checksum = checksum(expected_lines(blocks, logs))
                db = f'rgbench_{idx}'
                run('docker', 'exec', container, 'psql', '-U', 'postgres', '-c', f'CREATE DATABASE {db}')
                fixture = Fixture(blocks, logs, delay=.003 if smoke else 0)
                thread = threading.Thread(target=fixture.serve_forever, daemon=True)
                thread.start()
                try:
                    env = {**os.environ,
                        'REORGGUARD_RPC_HTTP_URL': f'http://127.0.0.1:{fixture.server_port}',
                        'REORGGUARD_DATABASE_URL': f'postgres://postgres@127.0.0.1:{port}/{db}?sslmode=disable&pool_max_conns=8',
                        'REORGGUARD_CHAIN_ID': str(CHAIN_ID), 'REORGGUARD_GENESIS_HASH': blocks[0]['hash'],
                        'REORGGUARD_START_BLOCK': '1', 'REORGGUARD_ADDRESS': ADDRESS,
                        'REORGGUARD_MAX_REORG_DEPTH': '8', 'REORGGUARD_SYNC_MODE': 'once',
                        'REORGGUARD_BACKFILL_WORKERS': str(workers)}
                    if workers > 1:
                        env['REORGGUARD_MAX_INFLIGHT_RANGES'] = str(inflight)
                    started_ns = time.perf_counter_ns()
                    with open(os.devnull, 'wb') as devnull:
                        process = subprocess.Popen([str(binary)], cwd=ROOT, env=env, stdout=devnull, stderr=devnull)
                        _, status, usage = os.wait4(process.pid, 0)
                        process.returncode = os.waitstatus_to_exitcode(status)
                    elapsed_ns = time.perf_counter_ns() - started_ns
                    elapsed = elapsed_ns / 1000000000
                    if process.returncode != 0:
                        raise RuntimeError(f'indexer failed for {density} workers={workers}')
                    actual = inspect_db(port, db)
                    if idx == 0:
                        result['environment']['indexes'] = run('psql', '-h', '127.0.0.1', '-p', str(port), '-U', 'postgres', '-d', db, '-At', '-c', "SELECT indexname FROM pg_indexes WHERE schemaname='public' ORDER BY indexname").splitlines()
                    expected = {'checkpoint': height, 'head_hash': blocks[-1]['hash'][2:],
                                'blocks': height + 1, 'logs': len(logs), 'duplicates': 0,
                                'parent_errors': 0, 'orphan_blocks': 0, 'orphan_logs': 0,
                                'checksum': reference_checksum}
                    if actual['checksum'] != expected['checksum']:
                        reference_lines = expected_lines(blocks, logs)
                        first = next(((a, b) for a, b in zip(reference_lines, actual['_lines']) if a != b), ('length mismatch', 'length mismatch'))
                        raise ValueError(f'checksum differs at {first}')
                    validate(expected, actual)
                    lat = list(fixture.latencies)
                    if smoke and workers > 1 and fixture.max_active < 2:
                        raise ValueError('parallel fetch did not overlap local RPC requests')
                    if smoke and elapsed > 30:
                        raise ValueError('benchmark smoke exceeded broad 30s catastrophic slowdown guardrail')
                    memory_bytes = usage.ru_maxrss if platform.system() == 'Darwin' else usage.ru_maxrss * 1024
                    record = {'run_id': run_identity(source_commit, idx, density, workers, height),
                              'run_index': idx, 'dataset_sha256': dataset_identity(density, height, reference_checksum),
                              'blocks_processed': height, 'logs_processed': len(logs),
                              'scenario': density, 'workers': workers, 'max_in_flight': inflight,
                              'wall_nanoseconds': elapsed_ns, 'wall_seconds': rounded_measurement(elapsed_ns),
                              'blocks_per_second': rounded_measurement(elapsed_ns, height),
                              'logs_per_second': rounded_measurement(elapsed_ns, len(logs)),
                              'db_rows_per_second': rounded_measurement(elapsed_ns, height + 1 + len(logs)),
                              'rpc_latency_p50_ms': percentile(lat, 50), 'rpc_latency_p95_ms': percentile(lat, 95),
                              'rpc_requests': fixture.requests, 'max_concurrent_rpc': fixture.max_active,
                              'range_requests': len(fixture.range_sizes),
                              'min_range_blocks': min(fixture.range_sizes), 'max_range_blocks': max(fixture.range_sizes),
                              'duplicate_rpc_deliveries': fixture.duplicate_deliveries, 'cpu_seconds': round(usage.ru_utime + usage.ru_stime, 4),
                              'cpu_percent_one_core': round((usage.ru_utime + usage.ru_stime) / elapsed * 100, 2),
                              'peak_process_rss_bytes': memory_bytes, 'checkpoint': actual['checkpoint'],
                              'canonical_blocks': actual['blocks'], 'canonical_logs': actual['logs'],
                              'duplicate_canonical_logs': actual['duplicates'],
                              'parent_errors': actual['parent_errors'], 'orphan_blocks': actual['orphan_blocks'],
                              'orphan_logs': actual['orphan_logs'], 'checksum': actual['checksum'],
                              'reference_checksum': reference_checksum, 'correctness': 'PASS'}
                    result['runs'].append(record)
                    print(json.dumps(record, sort_keys=True), flush=True)
                finally:
                    fixture.shutdown(); fixture.server_close(); thread.join(timeout=5)
        finally:
            run('docker', 'stop', container)
    if output:
        pathlib.Path(output).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(output).write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--smoke', action='store_true')
    parser.add_argument('--output')
    args = parser.parse_args()
    benchmark(args.smoke, args.output)
