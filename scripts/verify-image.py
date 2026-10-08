#!/usr/bin/env python3
"""Exercise the actual scratch image with local PostgreSQL and deterministic RPC."""
import json
import os
import pathlib
import subprocess
import sys
import threading
import time
import urllib.request
from benchmark import ADDRESS, CHAIN_ID, Fixture, dataset, run

IMAGE = 'reorgguard:phase7-local'


def docker(*args):
    return run('docker', *args)


def main():
    blocks, logs = dataset('sparse', 3)
    fixture = Fixture(blocks, logs, host='0.0.0.0')
    worker = threading.Thread(target=fixture.serve_forever, daemon=True)
    worker.start()
    db = None
    service = None
    network = None
    try:
        linux = sys.platform.startswith('linux')
        db_name = 'rg-db-' + str(os.getpid())
        if linux:
            network = 'rg-image-' + str(os.getpid())
            docker('network', 'create', network)
            db = docker('run', '-d', '--rm', '--network', network, '--name', db_name, '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', 'postgres:18.6')
            db_url = f'postgres://postgres@{db_name}:5432/postgres?sslmode=disable'
            network_flags = ['--network', network, '--add-host=host.docker.internal:host-gateway']
        else:
            db = docker('run', '-d', '--rm', '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', '-p', '127.0.0.1::5432', 'postgres:18.6')
            port = docker('port', db, '5432/tcp').split(':')[-1]
            db_url = f'postgres://postgres@host.docker.internal:{port}/postgres?sslmode=disable'
            network_flags = []
        for _ in range(100):
            try:
                if 'PostgreSQL init process complete; ready for start up.' not in subprocess.check_output(['docker', 'logs', db], stderr=subprocess.STDOUT, text=True):
                    raise RuntimeError('init in progress')
                docker('exec', db, 'psql', '-U', 'postgres', '-d', 'postgres', '-At', '-c', 'SELECT 1')
                break
            except RuntimeError:
                time.sleep(.1)
        else:
            raise RuntimeError('PostgreSQL unavailable')
        service = docker('run', '-d', *network_flags, '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges',
                         '-e', f'REORGGUARD_RPC_HTTP_URL=http://host.docker.internal:{fixture.server_port}',
                         '-e', f'REORGGUARD_DATABASE_URL={db_url}',
                         '-e', f'REORGGUARD_CHAIN_ID={CHAIN_ID}', '-e', f'REORGGUARD_GENESIS_HASH={blocks[0]["hash"]}',
                         '-e', 'REORGGUARD_START_BLOCK=1', '-e', f'REORGGUARD_ADDRESS={ADDRESS}',
                         '-e', 'REORGGUARD_MAX_REORG_DEPTH=8', '-e', 'REORGGUARD_SYNC_MODE=live',
                         '-e', 'REORGGUARD_POLL_INTERVAL=200ms', '-e', 'REORGGUARD_API_ADDR=127.0.0.1:8080', IMAGE)
        container_info = json.loads(docker('image', 'inspect', IMAGE))[0]
        if container_info['Config']['User'] != '65532:65532':
            raise RuntimeError('image is not configured as non-root')
        # A tiny helper shares the service network namespace. The service API
        # stays loopback-only; no host/public port is opened.
        url = 'http://127.0.0.1:8080'
        def fetch(path):
            probe = 'rg-probe-' + str(os.getpid())
            try:
                return docker('run', '--rm', '--name', probe, '--network', f'container:{service}',
                              'golang:1.27.1-alpine', 'wget', '-qO-', url + path)
            finally:
                subprocess.run(['docker', 'rm', '-f', probe], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            try:
                live = json.loads(fetch('/health/live'))
                ready = json.loads(fetch('/health/ready'))
                status = json.loads(fetch('/v1/status'))
                if live.get('status') == 'live' and ready.get('ready') and status.get('indexed_head') == 3:
                    break
            except (RuntimeError, ValueError):
                pass
            time.sleep(.1)
        else:
            details = docker('logs', '--tail', '12', service) if service else 'no container'
            raise RuntimeError(f'image liveness/readiness/catch-up failed: {details}')
        docker('stop', '--timeout', '5', service)
        exit_code = int(docker('inspect', '--format', '{{.State.ExitCode}}', service))
        if exit_code != 0:
            raise RuntimeError(f'image did not exit gracefully after SIGTERM: {exit_code}')
        docker('rm', service)
        service = None
        print('PASS scratch image: non-root, read-only rootfs, liveness, readiness, SIGTERM')
    finally:
        if service:
            subprocess.run(['docker', 'rm', '-f', service], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if db:
            docker('stop', db)
        if network:
            docker('network', 'rm', network)
        fixture.shutdown(); fixture.server_close(); worker.join(timeout=5)


if __name__ == '__main__':
    main()
