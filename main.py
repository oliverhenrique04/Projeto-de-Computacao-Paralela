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

Preparado para distribuição Docker:
  • DB_PATH via variável de ambiente CONNECTONGS_DB
  • Workers podem ser movidos para containers separados
  • A fila pode ser substituída por Redis/RabbitMQ pelo dev de distribuição
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
    from connectongs import logger as log

    BANNER = r"""
  ╔══════════════════════════════════════════════════════════════════╗
  ║      C O N N E C T O N G S  —  Doação de Alimentos              ║
  ║      Computação Paralela e Distribuída — Fase 2                  ║
  ╠══════════════════════════════════════════════════════════════════╣
  ║  [multiprocessing]  Semaphore · Lock · Queue · Pool · Process    ║
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

    def menu_interativo():
        db.init_db()
        users      = sim.seed_users()
        restaurant = next(u for u in users if u['user_type'] == 'RESTAURANTE')

        opcoes = {
            '1': ("Rodar TODAS as simulações",            run_all),
            '2': ("Cenário 1 — Logins simultâneos",
                  lambda: sim.cenario_logins_simultaneos(users)),
            '3': ("Cenário 2 — Race condition (corrida por alimento)",
                  lambda: sim.cenario_corrida_alimento(users, restaurant['id'])),
            '4': ("Cenário 3 — Notificações em paralelo (Pool)",
                  lambda: sim.cenario_notificacoes_lote(
                      users,
                      sim.seed_food(restaurant['id'])
                  )),
            '5': ("Cenário 4 — Fila + worker assíncrono",
                  lambda: sim.cenario_fila_notificacoes(users)),
            '6': ("Cenário 5 — Worker de expiração automática",
                  lambda: sim.cenario_expiracao_automatica(restaurant['id'])),
            '7': ("Ver relatório do banco de dados",      sim.print_report),
        }

        while True:
            print("\n" + "═" * 54)
            print("  CONNECTONGS — Menu de Simulações")
            print("═" * 54)
            for k, (desc, _) in opcoes.items():
                print(f"  [{k}] {desc}")
            print("  [0] Sair")
            print("═" * 54)

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
    else:
        menu_interativo()
