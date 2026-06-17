"""
Módulo de benchmark: detecta recursos do sistema e compara execução
serial vs concorrente vs distribuída (Docker).

Métricas coletadas:
  - CPUs físicos e threads lógicas (hiperthreading)
  - Tempo total de N operações em modo serial
  - Tempo total de N operações em modo concorrente (multiprocessing.Pool)
  - Latência média / mínima / máxima / p99 por operação
  - Throughput (ops/segundo)
  - Speedup em relação ao baseline serial
"""
import os
import sys
import time
import platform
import multiprocessing
from dataclasses import dataclass, field
from typing import List

from . import logger as log

# ─── Globals compartilhados pelos workers de Pool ─────────────────────────────

_bench_semaphore = None


def _init_bench_sem(sem):
    global _bench_semaphore
    _bench_semaphore = sem


def _bench_login_worker(args):
    """Worker top-level para Pool.map — executa 1 login e devolve latência."""
    email, password = args
    from connectongs import auth
    if _bench_semaphore is not None:
        auth.set_login_semaphore(_bench_semaphore)
    t0 = time.perf_counter()
    result = auth.login(email, password)
    return time.perf_counter() - t0, result is not None


# ─── Informações do sistema ───────────────────────────────────────────────────

@dataclass
class SystemInfo:
    cpu_logical: int       # threads lógicas (hyperthreading incluso)
    cpu_physical: int      # núcleos físicos
    hostname: str
    platform_str: str
    python_version: str
    pid: int

    def print_table(self):
        log.sep("INFORMAÇÕES DO SISTEMA — THREADS E CPUs")
        print(f"  Hostname              : {self.hostname}")
        print(f"  Plataforma            : {self.platform_str}")
        print(f"  Python                : {self.python_version}")
        print(f"  PID processo atual    : {self.pid}")
        print()
        print(f"  Núcleos físicos (CPU) : {self.cpu_physical}")
        print(f"  Threads lógicas       : {self.cpu_logical}")
        if self.cpu_logical > self.cpu_physical:
            ratio = self.cpu_logical // self.cpu_physical
            print(f"  Hyperthreading        : ATIVO ({ratio}× por núcleo)")
        else:
            print(f"  Hyperthreading        : não detectado")
        print()
        print(f"  Paralelismo máximo    : {self.cpu_logical} processos simultâneos")
        print(f"  Speedup teórico (max) : {self.cpu_logical:.1f}x sobre execução serial")
        log.sep()


def get_system_info() -> SystemInfo:
    """Detecta contagem de CPUs e threads do sistema atual via /proc/cpuinfo."""
    logical = os.cpu_count() or multiprocessing.cpu_count() or 1
    physical = logical

    # Leitura direta de /proc/cpuinfo (Linux) para núcleos físicos
    try:
        if os.path.exists('/proc/cpuinfo'):
            cores: set = set()
            pkg = None
            with open('/proc/cpuinfo') as fh:
                for line in fh:
                    line = line.strip()
                    if line.startswith('physical id'):
                        pkg = line.split(':')[1].strip()
                    elif line.startswith('core id') and pkg is not None:
                        cores.add((pkg, line.split(':')[1].strip()))
            if cores:
                physical = len(cores)
    except OSError:
        pass

    return SystemInfo(
        cpu_logical=logical,
        cpu_physical=physical,
        hostname=platform.node(),
        platform_str=platform.platform(),
        python_version=platform.python_version(),
        pid=os.getpid(),
    )


# ─── Resultado de benchmark ───────────────────────────────────────────────────

@dataclass
class BenchmarkResult:
    name: str
    n_ops: int
    n_workers: int
    total_time: float
    latencies: List[float] = field(default_factory=list)

    @property
    def throughput(self) -> float:
        return self.n_ops / self.total_time if self.total_time > 0 else 0.0

    @property
    def avg_latency_ms(self) -> float:
        return (sum(self.latencies) / len(self.latencies) * 1000) if self.latencies else 0.0

    @property
    def min_latency_ms(self) -> float:
        return min(self.latencies) * 1000 if self.latencies else 0.0

    @property
    def max_latency_ms(self) -> float:
        return max(self.latencies) * 1000 if self.latencies else 0.0

    @property
    def p99_latency_ms(self) -> float:
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        idx = min(int(len(s) * 0.99), len(s) - 1)
        return s[idx] * 1000


# ─── Benchmarks de login ──────────────────────────────────────────────────────

def run_serial(credentials: list, label: str = "Serial (1 worker)") -> BenchmarkResult:
    """
    Executa N logins sequencialmente — sem paralelismo.
    Linha de base para cálculo de speedup.
    """
    from connectongs import auth
    auth.set_login_semaphore(None)  # sem throttle no serial

    latencies = []
    t_start = time.perf_counter()

    for email, password in credentials:
        t0 = time.perf_counter()
        auth.login(email, password)
        latencies.append(time.perf_counter() - t0)

    total = time.perf_counter() - t_start

    return BenchmarkResult(
        name=label,
        n_ops=len(credentials),
        n_workers=1,
        total_time=total,
        latencies=latencies,
    )


def run_concurrent(credentials: list, n_workers: int,
                   sem_limit: int = None,
                   label: str = None) -> BenchmarkResult:
    """
    Executa N logins em paralelo usando multiprocessing.Pool.

    sem_limit: se fornecido, aplica Semaphore que limita concorrência real.
    """
    sem = multiprocessing.Semaphore(sem_limit) if sem_limit else None
    label = label or f"Concorrente ({n_workers} workers)"

    t_start = time.perf_counter()

    with multiprocessing.Pool(
        processes=n_workers,
        initializer=_init_bench_sem,
        initargs=(sem,),
    ) as pool:
        raw = pool.map(_bench_login_worker, credentials)

    total = time.perf_counter() - t_start
    latencies = [r[0] for r in raw]

    return BenchmarkResult(
        name=label,
        n_ops=len(credentials),
        n_workers=n_workers,
        total_time=total,
        latencies=latencies,
    )


# ─── Impressão comparativa ────────────────────────────────────────────────────

def print_comparison_table(results: List[BenchmarkResult]):
    """Imprime tabela ASCII comparativa com speedup em relação ao serial."""
    if not results:
        return

    baseline = results[0]

    log.sep("TABELA COMPARATIVA — Serial vs Concorrente vs Distribuído")

    col_w = [34, 8, 5, 10, 8, 16, 10]
    headers = ["Modo", "Workers", "Ops", "Tempo(s)", "Ops/s", "Lat.Média(ms)", "Speedup"]

    def row_fmt(vals):
        return "  " + "  ".join(str(v).ljust(w) if i < 2 else str(v).rjust(w)
                                 for i, (v, w) in enumerate(zip(vals, col_w)))

    print(row_fmt(headers))
    print("  " + "─" * (sum(col_w) + 2 * len(col_w)))

    for r in results:
        speedup = baseline.total_time / r.total_time if r.total_time > 0 else 0
        sp_str = "baseline" if r is baseline else f"{speedup:.2f}x"
        print(row_fmt([
            r.name,
            r.n_workers,
            r.n_ops,
            f"{r.total_time:.3f}",
            f"{r.throughput:.1f}",
            f"{r.avg_latency_ms:.1f}",
            sp_str,
        ]))

    print()
    best = max(results, key=lambda r: r.throughput)
    if best is not baseline:
        sp = baseline.total_time / best.total_time
        print(f"  Melhor modo  : {best.name}")
        print(f"  Speedup real : {sp:.2f}x  (teórico máximo: {get_system_info().cpu_logical:.1f}x)")
        eficiencia = sp / best.n_workers * 100
        print(f"  Eficiência   : {eficiencia:.1f}% por worker")
    log.sep()


def print_distributed_instructions(api_port: int = 8080):
    """Exibe instruções para rodar o benchmark distribuído via Docker."""
    log.sep("BENCHMARK DISTRIBUÍDO — Instruções Docker")
    print()
    print("  Para rodar workers distribuídos em containers Docker separados:")
    print()
    print("  1. Construir e iniciar (API + 4 workers):")
    print("     docker compose up --build")
    print()
    print("  2. Ver resultados de cada worker:")
    print("     docker compose logs -f worker_1 worker_2 worker_3 worker_4")
    print()
    print("  3. Encerrar:")
    print("     docker compose down")
    print()
    print("  ┌────────────────────────────────────────────────────────┐")
    print("  │  Arquitetura distribuída:                              │")
    print("  │                                                        │")
    print(f"  │  [worker_1] ──┐                                       │")
    print(f"  │  [worker_2] ──┤──► [api_server:{api_port}] ──► [SQLite]  │")
    print(f"  │  [worker_3] ──┤      ThreadingHTTPServer               │")
    print(f"  │  [worker_4] ──┘      Lock + Semaphore                 │")
    print("  └────────────────────────────────────────────────────────┘")
    print()
    print("  Cada worker container executa N requisições HTTP ao API server.")
    print("  O API server usa os mesmos mecanismos de concorrência do sistema")
    print("  (Semaphore, Lock, WAL mode) — agora acessíveis via rede.")
    log.sep()
