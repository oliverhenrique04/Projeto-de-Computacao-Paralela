"""
CONNECTONGS — Benchmark de Desempenho
Computação Paralela e Distribuída — Fase 2

Uso:
    python3 benchmark_runner.py              # roda benchmark serial + concorrente
    python3 benchmark_runner.py --full       # inclui todos os níveis de workers
    python3 benchmark_runner.py --docker     # exibe instruções para benchmark Docker
    python3 benchmark_runner.py --sysinfo    # apenas informações do sistema

Modos de benchmark:
  1. Serial        — operações executadas uma por vez (baseline)
  2. Concorrente   — multiprocessing.Pool com N workers
  3. Distribuído   — N containers Docker fazem requisições ao API server (ver --docker)
"""

import multiprocessing
import os
import sys
import time

# Guard multiprocessing (necessário no macOS/Windows com spawn; boa prática no Linux)
if __name__ == '__main__':
    from connectongs import database as db
    from connectongs import simulation as sim
    from connectongs import benchmark as bm
    from connectongs import logger as log

    BANNER = r"""
  ╔══════════════════════════════════════════════════════════════════╗
  ║      C O N N E C T O N G S  —  Benchmark de Desempenho          ║
  ║      Serial  ·  Concorrente  ·  Distribuído (Docker)            ║
  ╠══════════════════════════════════════════════════════════════════╣
  ║  multiprocessing.Pool  ·  Semaphore  ·  ThreadingHTTPServer     ║
  ╚══════════════════════════════════════════════════════════════════╝
    """
    print(BANNER)

    # ── 1. Informações do sistema ─────────────────────────────────────────────
    sysinfo = bm.get_system_info()
    sysinfo.print_table()

    if '--sysinfo' in sys.argv:
        sys.exit(0)

    if '--docker' in sys.argv:
        bm.print_distributed_instructions()
        sys.exit(0)

    # ── 2. Preparação dos dados ───────────────────────────────────────────────
    log.info("Inicializando banco de dados...")
    db.init_db()

    log.info("Garantindo usuários de teste...")
    users      = sim.seed_users()
    ongs       = [u for u in users if u['user_type'] == 'ONG']
    restaurant = next(u for u in users if u['user_type'] == 'RESTAURANTE')

    # Credenciais das ONGs para o benchmark de login (operação bcrypt — CPU-bound)
    credentials = [(u['email'], 'Senha@123') for u in ongs]
    n_ops = len(credentials)

    log.success(
        f"Setup: {n_ops} ONGs disponíveis para benchmark "
        f"| Restaurante ID {restaurant['id']}"
    )

    # ── 3. Benchmark de LOGIN (bcrypt é CPU-bound — ideal para mostrar speedup) ──

    log.sep("BENCHMARK 1 — Login de Usuários (bcrypt CPU-bound)")
    log.sim(f"Operação: autenticar {n_ops} ONGs com bcrypt.checkpw()")
    log.sim("Cada login inclui: acquire Semaphore → bcrypt.checkpw() → release")
    print()

    results_login = []

    # ── Serial ────────────────────────────────────────────────────────────────
    log.info(f"[1/N] Executando {n_ops} logins em modo SERIAL...")
    r_serial = bm.run_serial(credentials, label="Serial (1 worker)")
    results_login.append(r_serial)
    log.success(f"Serial concluído: {r_serial.total_time:.3f}s | {r_serial.throughput:.1f} ops/s")
    time.sleep(0.5)

    # ── Concorrente com 2 workers (= CPUs neste ambiente) ─────────────────────
    log.info(f"[2/N] Executando {n_ops} logins em modo CONCORRENTE (2 workers)...")
    r_conc2 = bm.run_concurrent(
        credentials, n_workers=2,
        label=f"Concorrente (2 workers)"
    )
    results_login.append(r_conc2)
    sp2 = r_serial.total_time / r_conc2.total_time
    log.success(
        f"Concorrente-2 concluído: {r_conc2.total_time:.3f}s | "
        f"{r_conc2.throughput:.1f} ops/s | speedup {sp2:.2f}x"
    )
    time.sleep(0.5)

    # ── Concorrente com Semaphore (throttle = 5, workers = n_ops) ────────────
    log.info(
        f"[3/N] Executando {n_ops} logins com SEMÁFORO "
        f"(throttle=5, {n_ops} workers)..."
    )
    r_sem = bm.run_concurrent(
        credentials, n_workers=n_ops,
        sem_limit=5,
        label=f"Concorrente c/ Semaphore(5)"
    )
    results_login.append(r_sem)
    sp_sem = r_serial.total_time / r_sem.total_time
    log.success(
        f"Semaphore concluído: {r_sem.total_time:.3f}s | "
        f"{r_sem.throughput:.1f} ops/s | speedup {sp_sem:.2f}x"
    )
    time.sleep(0.5)

    # ── Concorrente máximo (sem throttle) ─────────────────────────────────────
    if '--full' in sys.argv or sysinfo.cpu_logical > 2:
        max_w = min(n_ops, sysinfo.cpu_logical * 2)
        log.info(f"[4/N] Executando {n_ops} logins CONCORRENTE MAX ({max_w} workers)...")
        r_max = bm.run_concurrent(
            credentials, n_workers=max_w,
            label=f"Concorrente máx ({max_w} workers)"
        )
        results_login.append(r_max)
        sp_max = r_serial.total_time / r_max.total_time
        log.success(
            f"Max concluído: {r_max.total_time:.3f}s | "
            f"{r_max.throughput:.1f} ops/s | speedup {sp_max:.2f}x"
        )

    # ── Tabela comparativa ────────────────────────────────────────────────────
    bm.print_comparison_table(results_login)

    # ── 4. Análise de escalabilidade ──────────────────────────────────────────
    log.sep("ANÁLISE DE ESCALABILIDADE")
    print()
    print("  Lei de Amdahl — Estimativa de speedup máximo:")
    print()
    serial_fraction = 0.05  # ~5% do código é serial (init, DB, logging)
    for n in [1, 2, 4, 8, 16]:
        speedup_amdahl = 1 / (serial_fraction + (1 - serial_fraction) / n)
        print(
            f"  {n:>3} processadores → speedup teórico: "
            f"{speedup_amdahl:.2f}x  (limitado pela fração serial = {serial_fraction*100:.0f}%)"
        )
    print()
    print(f"  CPUs disponíveis neste host: {sysinfo.cpu_logical}")
    r_best = max(results_login, key=lambda r: r.throughput)
    if r_best is not results_login[0]:
        sp_real = r_serial.total_time / r_best.total_time
        efic = sp_real / r_best.n_workers * 100
        print(f"  Speedup real medido:  {sp_real:.2f}x com {r_best.n_workers} workers")
        print(f"  Eficiência paralela:  {efic:.1f}% por worker")
        print(
            f"  Overhead paralelo:    "
            f"{(r_best.n_workers - sp_real) / r_best.n_workers * 100:.1f}% "
            f"(fork + IPC + sincronização)"
        )
    log.sep()

    # ── 5. Instruções para benchmark distribuído ──────────────────────────────
    bm.print_distributed_instructions()

    log.success("Benchmark local concluído. Para benchmark distribuído: docker compose up --build")
