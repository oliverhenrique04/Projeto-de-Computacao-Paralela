"""
CONNECTONGS — Worker Cliente Distribuído (Docker)

Cada container Docker representa um nó independente que:
  1. Aguarda o API server ficar disponível
  2. Calcula sua fatia de TOTAL_REQUESTS automaticamente
  3. Busca os usuários correspondentes via GET /users/ongs
  4. Executa as requisições de login (serial ou concorrente)
  5. Reporta latência, throughput e speedup

Distribuição automática com TOTAL_REQUESTS=400 e 4 workers:
  Worker 1 (serial)      → 100 reqs | ONGs 1..100
  Worker 2 (serial)      → 100 reqs | ONGs 101..200
  Worker 3 (concorrente) → 100 reqs | ONGs 201..300
  Worker 4 (concorrente) → 100 reqs | ONGs 301..400

Variáveis de ambiente (configure em .env):
  TOTAL_REQUESTS — total de requisições divididas pelos 4 workers (padrão: 100)
  TOTAL_WORKERS  — número de workers (padrão: 4)
  WORKER_ID      — id deste worker 1..N (padrão: 1)
  MODE           — 'serial' ou 'concurrent' (padrão: serial)
  CONCURRENT_N   — threads simultâneas em modo concurrent (padrão: 5)
  API_URL        — URL do API server (padrão: http://api:8080)

Uso local (sem Docker):
  python3 worker_client.py --api-url http://localhost:8080 --total-requests 40
"""
import json
import os
import sys
import time
import argparse
import threading
import urllib.request
import urllib.error
from typing import List, Dict


SEED_PASSWORD = 'Teste@1234'


# ─── Argparse / Config ─────────────────────────────────────────────────────────

def _calc_per_worker(total: int, n_workers: int, wid: int) -> int:
    """Divide TOTAL_REQUESTS igualmente; o último worker pega o restante."""
    per = total // n_workers
    if wid == n_workers:
        return max(1, total - per * (n_workers - 1))
    return max(1, per)


def parse_args():
    p = argparse.ArgumentParser(description="CONNECTONGS Worker Distribuído")
    p.add_argument('--api-url',       default=os.environ.get('API_URL', 'http://api:8080'))
    p.add_argument('--worker-id',     type=int, default=int(os.environ.get('WORKER_ID', '1')))
    p.add_argument('--total-workers', type=int, default=int(os.environ.get('TOTAL_WORKERS', '4')))
    p.add_argument('--mode',          default=os.environ.get('MODE', 'serial'),
                   choices=['serial', 'concurrent'])
    p.add_argument('--concurrent-n',  type=int,
                   default=int(os.environ.get('CONCURRENT_N', '5')))

    # TOTAL_REQUESTS (preferido) divide entre workers; --n-requests define direto
    _total_env = int(os.environ.get('TOTAL_REQUESTS', '0'))
    _wid       = int(os.environ.get('WORKER_ID', '1'))
    _nw        = int(os.environ.get('TOTAL_WORKERS', '4'))
    _default_n = (
        _calc_per_worker(_total_env, _nw, _wid)
        if _total_env > 0
        else int(os.environ.get('N_REQUESTS', '25'))
    )
    p.add_argument('--total-requests', type=int,
                   default=_total_env,
                   help='Total de reqs divididas entre todos os workers')
    p.add_argument('--n-requests',     type=int, default=_default_n,
                   help='Reqs somente deste worker (ignorado se --total-requests > 0)')

    args = p.parse_args()

    # --total-requests tem prioridade e recalcula n-requests para este worker
    if args.total_requests > 0:
        args.n_requests = _calc_per_worker(
            args.total_requests, args.total_workers, args.worker_id
        )
    args.n_requests = max(1, min(args.n_requests, 1000))
    return args


# ─── HTTP helpers ──────────────────────────────────────────────────────────────

def _post(url: str, payload: dict, timeout: int = 20) -> tuple:
    """POST JSON → (status_code, response_dict, elapsed_ms)."""
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={'Content-Type': 'application/json',
                 'Content-Length': str(len(body))},
        method='POST',
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            elapsed = (time.perf_counter() - t0) * 1000
            return resp.status, json.loads(resp.read()), elapsed
    except urllib.error.HTTPError as exc:
        elapsed = (time.perf_counter() - t0) * 1000
        try:
            body_err = json.loads(exc.read())
        except Exception:
            body_err = {}
        return exc.code, body_err, elapsed


def _get(url: str, timeout: int = 15) -> tuple:
    """GET → (status_code, response_dict, elapsed_ms)."""
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            elapsed = (time.perf_counter() - t0) * 1000
            return resp.status, json.loads(resp.read()), elapsed
    except urllib.error.HTTPError as exc:
        elapsed = (time.perf_counter() - t0) * 1000
        return exc.code, {}, elapsed


# ─── Aguarda o API server ──────────────────────────────────────────────────────

def wait_for_api(base_url: str, wid: int, retries: int = 40, delay: float = 2.0) -> bool:
    print(f"[Worker-{wid}] Aguardando API em {base_url}/health ...", flush=True)
    for attempt in range(1, retries + 1):
        try:
            code, data, _ = _get(f"{base_url}/health", timeout=4)
            if code == 200:
                print(
                    f"[Worker-{wid}] API disponível "
                    f"(PID={data.get('pid')}, uptime={data.get('uptime_s')}s)",
                    flush=True
                )
                return True
        except Exception:
            pass
        print(f"[Worker-{wid}] [{attempt}/{retries}] Aguardando {delay}s...", flush=True)
        time.sleep(delay)
    print(f"[Worker-{wid}] ERRO: API não respondeu.", flush=True)
    return False


# ─── Busca a fatia de usuários deste worker ────────────────────────────────────

def fetch_my_users(base_url: str, wid: int, total_workers: int,
                   n_requests: int) -> List[Dict]:
    """
    Solicita ao API server a fatia de ONGs correspondente a este worker.
    Com 950 ONGs e 4 workers: cada worker recebe ~237 usuários.
    """
    code, data, _ = _get(f"{base_url}/users/ongs?limit=1&offset=0")
    if code != 200:
        print(f"[Worker-{wid}] Não foi possível buscar usuários: {data}", flush=True)
        return []

    total_ongs = data.get('total', 0)
    if total_ongs == 0:
        print(f"[Worker-{wid}] Nenhuma ONG encontrada no banco.", flush=True)
        return []

    # Divide os usuários entre os workers
    slice_size = total_ongs // total_workers
    offset = (wid - 1) * slice_size
    # Último worker pega o restante
    limit = slice_size if wid < total_workers else (total_ongs - offset)

    code, data, _ = _get(f"{base_url}/users/ongs?limit={limit}&offset={offset}")
    if code != 200:
        return []

    ongs = data.get('ongs', [])
    print(
        f"[Worker-{wid}] {len(ongs)} ONGs atribuídas "
        f"(offset={offset}, limit={limit}, total_banco={total_ongs})",
        flush=True
    )

    # Se n_requests > usuários disponíveis, cicla pela lista
    if n_requests > len(ongs):
        ongs = [ongs[i % len(ongs)] for i in range(n_requests)]
        ongs = [{'email': o['email'], 'id': o['id']} for o in ongs]
    else:
        ongs = [{'email': o['email'], 'id': o['id']} for o in ongs[:n_requests]]

    return ongs


# ─── Benchmark serial ─────────────────────────────────────────────────────────

def run_serial(base_url: str, wid: int, users: List[Dict]) -> List[float]:
    n = len(users)
    print(f"\n[Worker-{wid}] === SERIAL === {n} requisições de login\n", flush=True)
    latencies = []
    t_total = time.perf_counter()

    for idx, user in enumerate(users, 1):
        code, resp, lat = _post(
            f"{base_url}/auth/login",
            {'email': user['email'], 'password': SEED_PASSWORD}
        )
        latencies.append(lat)
        ok = "OK " if code == 200 else f"ERR({code})"
        # Imprime a cada 10 requisições para não poluir o log
        if idx % 10 == 0 or idx == n:
            throughput = idx / ((time.perf_counter() - t_total))
            print(
                f"[Worker-{wid}] {ok}  Req {idx:>4}/{n}  "
                f"lat={lat:>7.1f}ms  throughput={throughput:.1f} req/s",
                flush=True
            )

    total_s = time.perf_counter() - t_total
    print(
        f"\n[Worker-{wid}] Serial CONCLUÍDO: {n} reqs em {total_s:.2f}s "
        f"| {n/total_s:.1f} req/s",
        flush=True
    )
    return latencies


# ─── Benchmark concorrente (threads dentro do container) ──────────────────────

def run_concurrent(base_url: str, wid: int, users: List[Dict],
                    concurrency: int) -> List[float]:
    n = len(users)
    print(
        f"\n[Worker-{wid}] === CONCORRENTE === {n} reqs / {concurrency} threads\n",
        flush=True
    )
    latencies = [0.0] * n
    sem = threading.Semaphore(concurrency)
    lock = threading.Lock()
    completed = [0]

    def task(idx: int, user: Dict):
        with sem:
            code, resp, lat = _post(
                f"{base_url}/auth/login",
                {'email': user['email'], 'password': SEED_PASSWORD}
            )
            with lock:
                latencies[idx] = lat
                completed[0] += 1
                ok = "OK " if code == 200 else f"ERR({code})"
                if completed[0] % 10 == 0 or completed[0] == n:
                    print(
                        f"[Worker-{wid}] {ok}  Req {completed[0]:>4}/{n}  "
                        f"lat={lat:>7.1f}ms  thread={threading.current_thread().name}",
                        flush=True
                    )

    t_total = time.perf_counter()
    threads = [
        threading.Thread(
            target=task,
            args=(i, u),
            name=f"T{i % concurrency + 1:02d}"
        )
        for i, u in enumerate(users)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_s = time.perf_counter() - t_total
    print(
        f"\n[Worker-{wid}] Concorrente CONCLUÍDO: {n} reqs em {total_s:.2f}s "
        f"| {n/total_s:.1f} req/s  ({concurrency} threads)",
        flush=True
    )
    return latencies


# ─── Relatório final ──────────────────────────────────────────────────────────

def print_report(wid: int, mode: str, latencies: List[float], base_url: str):
    if not latencies:
        return

    n = len(latencies)
    total_ms = sum(latencies)
    avg = total_ms / n
    mn  = min(latencies)
    mx  = max(latencies)
    s_lat = sorted(latencies)
    p50 = s_lat[n // 2]
    p95 = s_lat[min(int(n * 0.95), n - 1)]
    p99 = s_lat[min(int(n * 0.99), n - 1)]
    throughput = n / (total_ms / 1000) if total_ms > 0 else 0

    # Estado do servidor
    try:
        _, stats, _ = _get(f"{base_url}/stats", timeout=5)
    except Exception:
        stats = {}

    sep = "═" * 60
    print(f"\n{sep}", flush=True)
    print(f"  RELATÓRIO FINAL — Worker-{wid} | Modo: {mode.upper()}", flush=True)
    print(f"{'─' * 60}", flush=True)
    print(f"  Requisições  : {n}", flush=True)
    print(f"  Throughput   : {throughput:.2f} req/s", flush=True)
    print(f"{'─' * 60}", flush=True)
    print(f"  Latência Avg : {avg:.1f} ms", flush=True)
    print(f"  Latência Min : {mn:.1f} ms", flush=True)
    print(f"  Latência Max : {mx:.1f} ms", flush=True)
    print(f"  Latência p50 : {p50:.1f} ms", flush=True)
    print(f"  Latência p95 : {p95:.1f} ms", flush=True)
    print(f"  Latência p99 : {p99:.1f} ms", flush=True)
    if stats:
        print(f"{'─' * 60}", flush=True)
        print(f"  Estado API   : {stats.get('users', '?')} usuários  "
              f"{stats.get('available_foods', '?')} alimentos  "
              f"{stats.get('reservations', '?')} reservas", flush=True)
    print(f"{sep}\n", flush=True)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    base = args.api_url.rstrip('/')
    wid  = args.worker_id

    total_label = (
        f" (total={args.total_requests} ÷ {args.total_workers} workers)"
        if args.total_requests > 0 else ""
    )
    print(f"\n{'═' * 60}", flush=True)
    print(f"  CONNECTONGS Worker-{wid}/{args.total_workers} | PID {os.getpid()}", flush=True)
    print(f"  API     : {base}", flush=True)
    print(f"  Modo    : {args.mode} | Reqs: {args.n_requests}{total_label}", flush=True)
    if args.mode == 'concurrent':
        print(f"  Threads : {args.concurrent_n} simultâneas", flush=True)
    print(f"{'═' * 60}\n", flush=True)

    if not wait_for_api(base, wid):
        sys.exit(1)

    users = fetch_my_users(base, wid, args.total_workers, args.n_requests)
    if not users:
        print(f"[Worker-{wid}] Nenhum usuário disponível — encerrando.", flush=True)
        sys.exit(1)

    if args.mode == 'serial':
        latencies = run_serial(base, wid, users)
    else:
        latencies = run_concurrent(base, wid, users, args.concurrent_n)

    print_report(wid, args.mode, latencies, base)


if __name__ == '__main__':
    main()
