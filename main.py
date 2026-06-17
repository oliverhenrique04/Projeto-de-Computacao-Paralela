"""
CONNECTONGS — Sistema de Doação de Alimentos
Projeto Integrador de Computação Paralela e Distribuída — Fase 2

Mecanismos de paralelismo implementados (multiprocessing):
  • multiprocessing.Semaphore  — throttle de logins simultâneos
  • multiprocessing.Lock       — seção crítica de reserva (evita double booking)
  • multiprocessing.Queue      — fila de notificações assíncronas
  • multiprocessing.Pool       — dispatch paralelo de notificações em lote
  • multiprocessing.Process    — workers daemon desacoplados (notificações + expiração)
  • SQLite UNIQUE INDEX        — garantia do banco contra race conditions

Distribuição via Docker:
  • API HTTP (ThreadingHTTPServer) — endpoint REST para clientes distribuídos
  • docker-compose.yml — orquestra API server + 4 worker containers
  • DB_PATH via variável de ambiente CONNECTONGS_DB
  • A fila pode ser substituída por Redis/RabbitMQ

Benchmark integrado:
  • Detecção de CPUs físicos e threads lógicas (/proc/cpuinfo)
  • Comparativo serial vs concorrente (multiprocessing.Pool)
  • Análise de speedup e eficiência (Lei de Amdahl)
"""

import multiprocessing
import os
import sys
import time

# Guard obrigatório para multiprocessing no Windows/macOS (spawn).
# No Linux (fork) é opcional, mas boa prática.
if __name__ == '__main__':
    # Importações internas ficam aqui para não executar no contexto de fork
    from connectongs import database as db
    from connectongs import simulation as sim
    from connectongs import benchmark as bm
    from connectongs import logger as log

    BANNER = r"""
  ╔══════════════════════════════════════════════════════════════════╗
  ║      C O N N E C T O N G S  —  Doação de Alimentos              ║
  ║      Computação Paralela e Distribuída — Fase 2                  ║
  ╠══════════════════════════════════════════════════════════════════╣
  ║  Semaphore · Lock · Queue · Pool · Process · Docker · Benchmark  ║
  ╚══════════════════════════════════════════════════════════════════╝
    """

    def run_all():
        log.sim(f"PID principal: {os.getpid()}")
        log.info("Inicializando banco de dados...")
        db.init_db()

        log.info("Criando usuários de teste (idempotente)...")
        users      = sim.seed_users()
        restaurant = next(u for u in users if u['user_type'] == 'RESTAURANTE')
        ongs_count = sum(1 for u in users if u['user_type'] == 'ONG')
        log.success(
            f"Setup: {ongs_count} ONGs + 1 Restaurante prontos. "
            f"Banco: {db.DB_PATH}"
        )

        time.sleep(0.3)
        sim.cenario_logins_simultaneos(users)

        time.sleep(0.3)
        sim.cenario_corrida_alimento(users, restaurant['id'])

        time.sleep(0.3)
        food = sim.seed_food(restaurant['id'], name="Marmitas do Dia")
        sim.cenario_notificacoes_lote(users, food)

        time.sleep(0.3)
        sim.cenario_fila_notificacoes(users)

        time.sleep(0.3)
        sim.cenario_expiracao_automatica(restaurant['id'])

        sim.print_report()
        log.success("Todas as simulações concluídas com sucesso.")

    def run_benchmark():
        """Executa benchmark serial vs concorrente com análise de speedup."""
        sysinfo = bm.get_system_info()
        sysinfo.print_table()

        log.info("Preparando dados para benchmark...")
        db.init_db()
        users = sim.seed_users()
        ongs  = [u for u in users if u['user_type'] == 'ONG']
        credentials = [(u['email'], 'Senha@123') for u in ongs]
        n_ops = len(credentials)

        log.sep("BENCHMARK — Login de Usuários (bcrypt CPU-bound)")
        log.sim(f"{n_ops} operações de login — Serial vs Concorrente")
        print()

        results = []

        log.info(f"[1/3] Serial — {n_ops} logins sequenciais...")
        r_serial = bm.run_serial(credentials)
        results.append(r_serial)
        log.success(f"Serial: {r_serial.total_time:.3f}s | {r_serial.throughput:.1f} ops/s")
        time.sleep(0.3)

        log.info(f"[2/3] Concorrente com {sysinfo.cpu_logical} workers...")
        r_conc = bm.run_concurrent(
            credentials, n_workers=sysinfo.cpu_logical,
            label=f"Concorrente ({sysinfo.cpu_logical} workers)"
        )
        results.append(r_conc)
        sp = r_serial.total_time / r_conc.total_time
        log.success(f"Concorrente: {r_conc.total_time:.3f}s | speedup {sp:.2f}x")
        time.sleep(0.3)

        log.info(f"[3/3] Concorrente com Semaphore(5) — {n_ops} workers...")
        r_sem = bm.run_concurrent(
            credentials, n_workers=n_ops, sem_limit=5,
            label="Concorrente c/ Semaphore(5)"
        )
        results.append(r_sem)
        sp_sem = r_serial.total_time / r_sem.total_time
        log.success(f"Sem.(5): {r_sem.total_time:.3f}s | speedup {sp_sem:.2f}x")

        bm.print_comparison_table(results)
        bm.print_distributed_instructions()

    def menu_interativo():
        db.init_db()
        users      = sim.seed_users()
        restaurant = next(u for u in users if u['user_type'] == 'RESTAURANTE')

        opcoes = {
            '1': ("Rodar TODAS as simulações",                  run_all),
            '2': ("Cenário 1 — Logins simultâneos",
                  lambda: sim.cenario_logins_simultaneos(users)),
            '3': ("Cenário 2 — Race condition (corrida por alimento)",
                  lambda: sim.cenario_corrida_alimento(users, restaurant['id'])),
            '4': ("Cenário 3 — Notificações em paralelo (Pool)",
                  lambda: sim.cenario_notificacoes_lote(
                      users, sim.seed_food(restaurant['id'])
                  )),
            '5': ("Cenário 4 — Fila + worker assíncrono",
                  lambda: sim.cenario_fila_notificacoes(users)),
            '6': ("Cenário 5 — Worker de expiração automática",
                  lambda: sim.cenario_expiracao_automatica(restaurant['id'])),
            '7': ("Ver relatório do banco de dados",            sim.print_report),
            '8': ("Informações do sistema (CPUs / Threads)",
                  lambda: bm.get_system_info().print_table()),
            '9': ("Benchmark — Serial vs Concorrente vs Distribuído",
                  run_benchmark),
        }

        while True:
            print("\n" + "═" * 62)
            print("  CONNECTONGS — Menu de Simulações e Benchmark")
            print("═" * 62)
            for k, (desc, _) in opcoes.items():
                print(f"  [{k}] {desc}")
            print("  [0] Sair")
            print("═" * 62)

            choice = input("  Escolha: ").strip()
            if choice == '0':
                print("  Encerrando.")
                break
            elif choice in opcoes:
                try:
                    opcoes[choice][1]()
                except KeyboardInterrupt:
                    log.warning("Interrompido pelo usuário.")
            else:
                print("  Opção inválida.")

    # ── Entry point ────────────────────────────────────────────────
    print(BANNER)

    if '--auto' in sys.argv or '-a' in sys.argv:
        # Modo automático: roda tudo sem interação (útil para demos e CI)
        run_all()
    elif '--benchmark' in sys.argv or '-b' in sys.argv:
        # Modo benchmark: apenas executa comparativo de desempenho
        db.init_db()
        run_benchmark()
    elif '--sysinfo' in sys.argv:
        # Exibe apenas informações do sistema
        bm.get_system_info().print_table()
    else:
        menu_interativo()
