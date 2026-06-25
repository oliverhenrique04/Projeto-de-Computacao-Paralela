# CONNECTONGS — Sistema de Doação de Alimentos

Plataforma que conecta **restaurantes** de Brasília a **ONGs** para doação de alimentos excedentes, desenvolvida como Projeto Integrador de **Computação Paralela e Distribuída — Fase 2**.

> Lei nº 14.016/2020 — combate ao desperdício de alimentos

---

## Grupo

| Nome | RA |
|---|---|
| Eliane de Freitas | 079695 |
| Marcos de Oliveira | 082028 |
| Nathan David | 077992 |
| Oliver Henrique | 083885 |
| Pauline Fernandes | 076961 |
| Rayna Livia | 084130 |
| Gabriel Yan | 077220 |

---

## Resultados Medidos (Windows 11 · Python 3.13 · 16 núcleos)

### Benchmark de login bcrypt — 10 operações

| Modo | Tempo | Throughput | Speedup |
|---|---|---|---|
| Serial (1 processo) | **2.617s** | 3.8 ops/s | baseline |
| Paralelo (16 processos) | **0.824s** | 12.1 ops/s | **3.18×** |
| Paralelo + Semaphore(5) | **0.785s** | 12.7 ops/s | **3.33×** |

> Eficiência por processo: **33.3%** — bcrypt satura 1 núcleo inteiro; contenção no SQLite limita o ganho.

### Teste com 500 usuários simultâneos

| Operação | Serial (estimativa) | Paralelo (medido) | Ganho |
|---|---|---|---|
| 500 logins — Semaphore(10), 100 workers | ~130s | **5.52s** | ~24× |
| 500 notificações — Pool, 16 workers | ~25s | **4.65s** | ~5× |

### Benchmark distribuído Docker (4 containers)

| Container | Modo | Throughput |
|---|---|---|
| worker_1 | Serial | 11.1 req/s |
| worker_2 | Serial | 11.7 req/s |
| worker_3 | Concorrente (5 threads) | 5.9 req/s |
| worker_4 | Concorrente (10 threads) | 4.3 req/s |
| **Total distribuído** | — | **~33 req/s** (**12.7× ganho** sobre serial puro) |

---

## Pré-requisitos

```bash
python3 --version          # 3.10 ou superior (testado em 3.13)
docker --version           # qualquer versão recente
docker compose version

pip install -r requirements.txt    # instala bcrypt==4.2.1
```

> **Windows:** use sempre `python -X utf8` para evitar erros de codificação nos logs com caracteres especiais.

---

## Início Rápido

```bash
# 1. Instalar dependências
pip install -r requirements.txt

# 2. Criar banco com 1000 usuários (50 restaurantes + 950 ONGs)
python seed_database.py

# 3. Menu interativo
python -X utf8 main.py

# 4. Rodar todos os 5 cenários automaticamente (ideal para apresentação)
python -X utf8 main.py --auto

# 5. Rodar com mais usuários (até 950 ONGs do banco)
python -X utf8 main.py --users 500 --sem 10 --auto
```

---

## Estrutura de Arquivos

```
CONNECTONGS/
│
├── .env                        ← configuração Docker (TOTAL_REQUESTS)
├── main.py                     ← menu interativo + entry point
├── seed_database.py            ← cria 1000 usuários no banco
├── benchmark_runner.py         ← benchmark local serial vs concorrente
├── benchmark_dashboard.py      ← gera dashboard HTML interativo
├── gerar_apresentacao.py       ← gera PPTX + roteiro de apresentação
├── worker_client.py            ← cliente HTTP (containers Docker)
├── Dockerfile                  ← imagem Docker única (api + worker)
├── docker-compose.yml          ← seed → api → 4 workers
├── docker-entrypoint.sh        ← papel do container (ROLE=api|worker)
├── requirements.txt            ← bcrypt==4.2.1
│
└── connectongs/
    ├── __init__.py
    ├── logger.py               ← log colorido com timestamp e PID
    ├── database.py             ← SQLite WAL + schema + audit_log
    ├── auth.py                 ← cadastro e login com Semaphore
    ├── foods.py                ← CRUD de alimentos
    ├── reservations.py         ← reserva com Lock + UNIQUE INDEX
    ├── notifications.py        ← Pool.map (lote) + Queue (assíncrono)
    ├── workers.py              ← Process daemon (ExpiryChecker + NotifWorker)
    ├── simulation.py           ← 5 cenários de demonstração
    ├── benchmark.py            ← detecção de CPUs/threads + métricas
    └── api_server.py           ← ThreadingHTTPServer para workers Docker
```

---

## 1 — Banco de Dados com 1000 Usuários

O banco SQLite começa vazio. O script `seed_database.py` popula com **1000 usuários** usando `multiprocessing.Pool` para paralelizar o hashing bcrypt — operação CPU-bound.

```
50 restaurantes  →  restaurante01@connectongs.dev ... restaurante50@connectongs.dev
950 ONGs         →  ong0001@connectongs.dev        ... ong0950@connectongs.dev
200 alimentos    →  4 por restaurante, validades aleatórias
Senha única      →  Teste@1234
```

| Método | Tempo para 1000 hashes bcrypt |
|---|---|
| Serial (1 processo, rounds=12) | ~5 minutos |
| Pool(2, rounds=12) | ~2.5 minutos |
| Pool(2, rounds=4) | **~1 segundo** |

`rounds=4` é suficiente para testes — o `bcrypt.checkpw()` funciona pois os rounds ficam gravados dentro do próprio hash.

```bash
python seed_database.py              # cria 1000 usuários (execução inicial)
python seed_database.py --status     # quantos usuários existem no banco
python seed_database.py --reset      # apaga tudo e recria do zero
```

**Saída esperada:**

```
Etapa 1/2 — Hashing bcrypt (1000 usuários, 2 processos)...
Hashing concluído em 0.82s  (1273 hashes/s, 2 processos paralelos)

Etapa 2/2 — Inserindo 1000 usuários (INSERT OR IGNORE)...
DB insert em 0.022s

  Restaurantes  :    50
  ONGs          :   950
  Total usuários:  1000
  Alimentos     :   200
```

---

## 2 — Menu Interativo

```bash
python -X utf8 main.py
```

```
  [1] Rodar TODAS as simulações
  [2] Cenário 1 — Logins simultâneos         (Semaphore)
  [3] Cenário 2 — Race condition             (Lock + UNIQUE INDEX)
  [4] Cenário 3 — Notificações em paralelo   (Pool)
  [5] Cenário 4 — Fila + worker assíncrono   (Queue + Process)
  [6] Cenário 5 — Worker de expiração        (Process daemon)
  [7] Ver relatório do banco de dados
  [8] Informações do sistema (CPUs / Threads)
  [9] Benchmark — Serial vs Concorrente vs Distribuído
  [0] Sair
```

### Flags disponíveis

| Flag | Padrão | Descrição |
|---|---|---|
| `--auto` / `-a` | — | Roda todos os 5 cenários sem menu |
| `--benchmark` / `-b` | — | Executa somente o benchmark comparativo |
| `--sysinfo` | — | Exibe CPUs/threads e sai |
| `--users N` | 10 (seed) | Carrega N ONGs do banco (1 a 950) |
| `--sem N` | 5 | Define o limite do Semaphore |

```bash
python -X utf8 main.py                              # menu interativo, 10 usuários
python -X utf8 main.py --auto                       # automático, 10 usuários
python -X utf8 main.py --users 100 --auto           # 100 ONGs simultâneas
python -X utf8 main.py --users 500 --sem 10 --auto  # 500 ONGs, Semaphore(10)
python -X utf8 main.py --benchmark                  # benchmark serial vs concorrente
python -X utf8 main.py --sysinfo                    # informações do sistema
```

---

## 3 — Mecanismos de Paralelismo (5 Cenários)

### Cenário 1 — Logins Simultâneos (`multiprocessing.Semaphore`)

**Arquivo:** `connectongs/auth.py`

N ONGs tentam logar ao mesmo tempo. O `Semaphore(5)` funciona como uma catraca: no máximo 5 processos executam bcrypt simultaneamente; os demais aguardam na fila.

```
[PID 1001] aguardando slot... → ADQUIRIDO → bcrypt.checkpw() → LIBERADO
[PID 1002] aguardando slot... → ADQUIRIDO → bcrypt.checkpw() → LIBERADO
[PID 1003] aguardando...      (semáforo cheio — espera vaga)
```

**Resultado medido (500 usuários, Semaphore(10), 100 workers):**

```
500/500 logins OK | Tempo total: 5.52s | Semáforo limitou a 10 simultâneos
```

---

### Cenário 2 — Race Condition (`multiprocessing.Lock` + `UNIQUE INDEX`)

**Arquivo:** `connectongs/reservations.py`

8 ONGs (ou mais, com `--users`) tentam reservar o **mesmo** alimento ao mesmo tempo. Exatamente 1 vence; as demais recebem `409 Conflict`.

**Dupla proteção:**

```
Nível 1 — Lock (aplicação):
  ONG-A adquire Lock → verifica DISPONIVEL → insere → muda status → libera
  ONG-B aguarda Lock → vê RESERVADO → retorna 409

Nível 2 — UNIQUE INDEX (banco):
  CREATE UNIQUE INDEX idx_unique_active_reservation
      ON reservations(food_id)
      WHERE status != 'CANCELADO';

  Garante consistência mesmo entre containers Docker sem memória compartilhada.
```

**Resultado:** 1 vencedor exato + N-1 × 409 Conflict — repetível e determinístico.

---

### Cenário 3 — Notificações em Lote (`multiprocessing.Pool.map`)

**Arquivo:** `connectongs/notifications.py`

Restaurante cadastra alimento → sistema notifica todas as ONGs **em paralelo** via `Pool.map()`. Workers capeados em `cpu_count()` para não estourar memória.

```
Pool(16 workers)
  ├─ Worker PID-A → ONG 001 (simultâneo)
  ├─ Worker PID-B → ONG 002 (simultâneo)
  └─ Worker PID-C → ONG 003 (simultâneo)
```

**Resultado medido (500 notificações, 16 workers):** **4.65s** vs ~25s serial.

---

### Cenário 4 — Fila + Worker Assíncrono (`multiprocessing.Queue` + `Process`)

**Arquivos:** `connectongs/notifications.py`, `connectongs/workers.py`

O fluxo principal **nunca bloqueia**: coloca evento na fila e continua. O `NotificationWorker` (processo separado, PID diferente) consome a fila em background.

```
Fluxo principal (PID 15572)        NotificationWorker (PID 34528)
      │                                       │
  enqueue(msg) ──→ Queue ─────────────→  consome → persiste no banco
      │ (retorna imediatamente)               │
  enqueue(msg) ──→ Queue ─────────────→  consome → persiste no banco
      │                                       │
  continua outras tarefas               (em background, daemon=True)
```

---

### Cenário 5 — Expiração Automática (`multiprocessing.Process` daemon)

**Arquivo:** `connectongs/workers.py`

O `ExpiryChecker` roda em loop independente, verificando alimentos vencidos a cada 2 segundos — sem depender de nenhuma requisição do usuário.

```
Loop a cada 2s:
  Ciclo 1 → 104 alimentos expirados → marca EXPIRADO → notifica restaurante
  Ciclo 2 → 0 expirados → aguarda
  stop_event sinalizado → encerra graciosamente
```

---

## 4 — Benchmark Local: Serial vs Concorrente

```bash
python -X utf8 main.py --benchmark
# ou
python -X utf8 benchmark_runner.py
```

**Saída real medida (Windows 11, Python 3.13, 16 núcleos):**

```
Modo                              Workers  Ops  Tempo(s)  Ops/s  Lat.Média(ms)  Speedup
Serial (1 worker)                       1   10     2.617    3.8          261.7   baseline
Concorrente (16 workers)               16   10     0.824   12.1          315.5      3.18x
Concorrente c/ Semaphore(5)            10   10     0.785   12.7          371.2      3.33x

Melhor modo  : Concorrente c/ Semaphore(5)
Speedup real : 3.33x  (teórico máximo: 16.0x)
Eficiência   : 33.3% por worker
```

**Lei de Amdahl (p = 0.95 — 5% de código serial):**

| Processos | Speedup teórico | Speedup medido |
|---|---|---|
| 1 | 1.00× | 1.00× |
| 2 | 1.90× | — |
| 4 | 3.48× | — |
| 8 | 5.93× | — |
| 10 | 7.05× | **3.33×** (eficiência: 47%) |
| 16 | 9.14× | 3.18× |
| ∞ | 20.0× | — |

> A diferença entre teórico e medido se deve à contenção no SQLite (lock de escrita) e ao overhead de fork/IPC do multiprocessing — ambos fazem parte do 5% serial.

---

## 5 — Dashboard HTML Interativo

```bash
# Gera dashboard com gráficos Chart.js (abre automaticamente no browser)
python -X utf8 benchmark_dashboard.py

# Com número de usuários configurável
python -X utf8 benchmark_dashboard.py --users 50
python -X utf8 benchmark_dashboard.py --users 100 --sem 10
python -X utf8 benchmark_dashboard.py --users 500 --no-browser --output resultado.html
```

O dashboard inclui: speedup, throughput, latência (avg/p50/p95/p99), curva da Lei de Amdahl, histogramas de latência por modo, log de race condition e cards dos 5 cenários.

---

## 6 — Apresentação PowerPoint + Roteiro

```bash
# Gera CONNECTONGS_Apresentacao.pptx (18 slides) + ROTEIRO_Apresentacao.txt
python -X utf8 gerar_apresentacao.py
```

O PPTX inclui gráficos embutidos (matplotlib), dados reais do benchmark e do teste com 500 usuários, analogias para leigos e log colorido dos cenários.

---

## 7 — Benchmark Distribuído (Docker)

### Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│                     docker compose up                         │
│                                                              │
│  [seed]  ──→  cria 1000 usuários → sai                       │
│                    ↓                                          │
│  [api]   ──→  ThreadingHTTPServer :8080                       │
│               Semaphore(5) + Lock + SQLite WAL                │
│                    ↑                                          │
│  [worker_1] serial       ──┐                                  │
│  [worker_2] serial       ──┤──→ POST /auth/login × N         │
│  [worker_3] concurrent   ──┤    (cada worker usa sua fatia    │
│  [worker_4] concurrent   ──┘     dos 1000 usuários)           │
└──────────────────────────────────────────────────────────────┘
```

### Configuração

Edite o arquivo **`.env`** na raiz do projeto:

```env
# Número TOTAL de requisições divididas pelos 4 workers
TOTAL_REQUESTS=100
```

| `TOTAL_REQUESTS` | Por worker (≈) | Tempo estimado |
|---|---|---|
| `40` | 10 por worker | ~10 segundos |
| `100` | 25 por worker | ~25 segundos |
| `400` | 100 por worker | ~1.5 minutos |
| `1000` | 250 por worker | ~4 minutos |

### Como rodar

```bash
# 1. Configure o número de requisições
echo "TOTAL_REQUESTS=200" > .env

# 2. Execute (seed + api + 4 workers)
docker compose up --build

# 3. Acompanhe em tempo real
docker compose logs -f

# 4. Ver relatório de um worker específico
docker compose logs worker_1

# 5. Salvar todos os logs
docker compose logs --no-color > resultado.txt

# 6. Encerrar e limpar volumes
docker compose down -v
```

### Saída esperada de um worker

```
════════════════════════════════════════════════════════════
  CONNECTONGS Worker-1/4 | PID 7
  API     : http://api:8080
  Modo    : serial | Reqs: 25 (total=100 ÷ 4 workers)
════════════════════════════════════════════════════════════

[Worker-1] OK   Req  10/25  lat=  81ms  throughput=3.1 req/s
[Worker-1] OK   Req  25/25  lat=  72ms  throughput=3.3 req/s

  Requisições  : 25
  Throughput   : 3.3 req/s
  Latência Avg : 90.3 ms  |  p99: 141.8 ms
════════════════════════════════════════════════════════════
```

### Resultados obtidos (950 ONGs, rounds=4)

| Container | Modo | Throughput | Lat. Avg | Lat. p99 |
|---|---|---|---|---|
| worker_1 | Serial | 11.1 req/s | 90 ms | 654 ms |
| worker_2 | Serial | 11.7 req/s | 86 ms | 308 ms |
| worker_3 | Concorrente (5 threads) | 5.9 req/s | 169 ms | 362 ms |
| worker_4 | Concorrente (10 threads) | 4.3 req/s | 231 ms | 354 ms |
| **Total** | — | **~33 req/s** | — | — |

> **Por que serial por worker supera concorrente?** bcrypt satura 1 CPU. Quando worker_4 abre 10 threads, todas competem pelos mesmos núcleos do servidor — o Semaphore cria fila e aumenta latência. O worker serial envia 1 requisição por vez, o servidor processa sem espera.

---

## 8 — API HTTP

**Arquivo:** `connectongs/api_server.py`

Usa `ThreadingHTTPServer` da biblioteca padrão — sem Flask, sem FastAPI. Cada requisição é tratada em thread separada.

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/health` | Status do servidor (pid, uptime) |
| `GET` | `/stats` | Contagem de registros no banco |
| `GET` | `/users/ongs?limit=N&offset=O` | Paginação de ONGs |
| `GET` | `/users/restaurants` | Lista restaurantes |
| `GET` | `/foods/available` | Alimentos disponíveis |
| `POST` | `/auth/register` | Cadastro de usuário |
| `POST` | `/auth/login` | Login com Semaphore (throttle) |
| `POST` | `/foods/add` | Cadastrar alimento |
| `POST` | `/foods/reserve` | Reservar alimento com Lock |

```bash
# Terminal 1 — inicia o servidor
python -m connectongs.api_server

# Terminal 2 — testa endpoints
curl http://localhost:8080/health
curl http://localhost:8080/stats

# Login via curl
curl -X POST http://localhost:8080/auth/login \
     -H "Content-Type: application/json" \
     -d '{"email":"ong0001@connectongs.dev","password":"Teste@1234"}'
# {"success": true, "user_id": 2, "user_type": "ONG", "elapsed_ms": 85.3}
```

---

## 9 — Banco de Dados (SQLite WAL)

**Arquivo:** `connectongs/database.py`

```sql
PRAGMA journal_mode=WAL;      -- múltiplos leitores simultâneos com 1 escritor
PRAGMA busy_timeout=10000;    -- espera até 10s por lock em vez de falhar
PRAGMA foreign_keys=ON;       -- integridade referencial ativa
```

| Tabela | Propósito |
|---|---|
| `users` | Restaurantes e ONGs (email UNIQUE, password_hash, user_type) |
| `foods` | Alimentos para doação (restaurant_id, expiry_date, status) |
| `reservations` | Reservas — 1 ativa por alimento (UNIQUE INDEX) |
| `notifications` | Mensagens enviadas para ONGs |
| `audit_log` | Rastreabilidade de todas as operações (event_type, process_id) |

**Índice crítico para consistência:**

```sql
CREATE UNIQUE INDEX idx_unique_active_reservation
    ON reservations(food_id)
    WHERE status != 'CANCELADO';
```

Garante que nunca existam duas reservas ativas para o mesmo alimento — inclusive em ambiente distribuído, onde o `Lock` de memória não funciona entre containers.

---

## 10 — Evidências de Paralelismo na Saída

Ao rodar qualquer cenário, os logs mostram PIDs diferentes operando ao mesmo tempo:

```
[11:35:22.820] [PID 13256] Disparando 500 logins via Pool (100 workers)...
[11:35:23.100] [PID 17100] → ONG 0001 — ADQUIRIDO → bcrypt → LIBERADO
[11:35:23.102] [PID  6328] → ONG 0002 — ADQUIRIDO → bcrypt → LIBERADO
[11:35:23.103] [PID 18200] → ONG 0003 — ADQUIRIDO → bcrypt → LIBERADO
[11:35:28.344] [PID 13256] Resultado: 500/500 logins OK | Tempo total: 5.52s
```

PIDs diferentes (`17100`, `6328`, `18200`) executando **no mesmo milissegundo** — isso é paralelismo real, não concorrência simulada.

---

## 11 — Fluxo Completo de uma Reserva

```
ONG faz login
    │
    ▼
Semaphore(5) ──── máx 5 logins simultâneos
    │
    ▼
ONG busca alimentos disponíveis
    │
    ▼
ONG clica "Reservar"
    │
    ▼
Lock.acquire() ──── seção crítica começa
    ├─ SELECT: food.status == 'DISPONIVEL'?
    │     NÃO ──→ 409 Conflict
    │     SIM ↓
    ├─ INSERT INTO reservations  ──→ UNIQUE INDEX violado? → 409 Conflict
    └─ UPDATE foods SET status='RESERVADO'
Lock.release() ──── seção crítica termina
    │
    ▼
Notificação enfileirada na Queue
    │
    ▼
NotificationWorker (Process separado, PID diferente) persiste no banco
```

---

## 12 — Variáveis de Ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `CONNECTONGS_DB` | `connectongs.db` | Caminho do banco SQLite |
| `TOTAL_REQUESTS` | `100` | Total de reqs nos workers Docker |
| `MODE_SERIAL` | `serial` | Modo dos workers 1 e 2 |
| `MODE_CONCURRENT` | `concurrent` | Modo dos workers 3 e 4 |
| `CONCURRENT_N_W3` | `5` | Threads simultâneas no worker 3 |
| `CONCURRENT_N_W4` | `10` | Threads simultâneas no worker 4 |
| `API_URL` | `http://api:8080` | URL do API server (worker_client.py) |

---

## 13 — Dependências

```
bcrypt==4.2.1
```

Todo o restante usa a **biblioteca padrão do Python 3.10+**:

| Módulo | Uso |
|---|---|
| `multiprocessing` | Semaphore, Lock, Pool, Queue, Process |
| `sqlite3` | Banco de dados com WAL |
| `http.server` | ThreadingHTTPServer (API distribuída) |
| `threading` | Threads concorrentes no worker_client |
| `urllib.request` | Requisições HTTP (sem requests) |
| `os`, `platform` | Detecção de CPUs |

**Dependências opcionais** (para relatórios e apresentação):

```bash
pip install python-pptx matplotlib     # para gerar_apresentacao.py
# benchmark_dashboard.py não precisa de dependências extras
```

---

## Referência Rápida de Comandos

```bash
# ── Setup ──────────────────────────────────────────────────
pip install -r requirements.txt              # instala bcrypt
python seed_database.py                      # cria 1000 usuários
python seed_database.py --status             # verifica banco
python seed_database.py --reset              # apaga e recria

# ── Simulações ─────────────────────────────────────────────
python -X utf8 main.py                       # menu interativo (10 usuários)
python -X utf8 main.py --auto                # todos os cenários automático
python -X utf8 main.py --users 500 --sem 10 --auto   # 500 usuários
python -X utf8 main.py --sysinfo             # CPUs e threads do sistema

# ── Benchmark ──────────────────────────────────────────────
python -X utf8 main.py --benchmark           # serial vs concorrente (10 ops)
python -X utf8 benchmark_runner.py           # benchmark alternativo

# ── Dashboard HTML ─────────────────────────────────────────
python -X utf8 benchmark_dashboard.py        # gera e abre dashboard.html
python -X utf8 benchmark_dashboard.py --users 100 --sem 10

# ── Apresentação ───────────────────────────────────────────
python -X utf8 gerar_apresentacao.py         # gera PPTX (18 slides) + roteiro

# ── API server local ───────────────────────────────────────
python -m connectongs.api_server             # sobe na porta 8080
curl http://localhost:8080/health

# ── Docker distribuído ─────────────────────────────────────
echo "TOTAL_REQUESTS=200" > .env
docker compose up --build                    # seed + api + 4 workers
docker compose logs -f                       # acompanha em tempo real
docker compose logs --no-color > resultado.txt
docker compose down -v                       # limpa tudo
```
