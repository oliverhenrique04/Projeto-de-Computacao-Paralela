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

## Visão Geral

O projeto implementa **três camadas de paralelismo e distribuição**:

| Camada | Mecanismo | Onde rodar |
|---|---|---|
| **Paralelismo local** | `multiprocessing` (Semaphore, Lock, Pool, Queue, Process) | `main.py` |
| **Benchmark comparativo** | Serial vs Concorrente com medição de speedup | `benchmark_runner.py` |
| **Distribuição** | 4 containers Docker fazendo requisições a um API server | `docker compose` |

---

## Pré-requisitos

```bash
python3 --version   # 3.10 ou superior
docker --version    # qualquer versão recente
docker compose version
pip install -r requirements.txt   # instala bcrypt==4.2.1
```

---

## Início Rápido

```bash
# 1. Instalar dependências
pip install -r requirements.txt

# 2. Criar banco com 1000 usuários
python3 seed_database.py

# 3. Menu interativo com todas as opções
python3 main.py

# 4. Rodar todos os 5 cenários de uma vez (apresentação)
python3 main.py --auto
```

---

## Estrutura de Arquivos

```
CONNECTONGS/
│
├── .env                    ← configure aqui (TOTAL_REQUESTS)
├── main.py                 ← menu interativo + entry point
├── seed_database.py        ← cria 1000 usuários no banco
├── benchmark_runner.py     ← benchmark local serial vs concorrente
├── worker_client.py        ← cliente HTTP (roda em containers Docker)
├── Dockerfile              ← imagem Docker única (api + worker)
├── docker-compose.yml      ← orquestração: seed → api → 4 workers
├── docker-entrypoint.sh    ← define papel do container (ROLE=api|worker)
├── requirements.txt        ← bcrypt==4.2.1
│
└── connectongs/
    ├── __init__.py
    ├── logger.py           ← log colorido com timestamp e PID
    ├── database.py         ← SQLite WAL + schema + audit_log
    ├── auth.py             ← cadastro e login com Semaphore
    ├── foods.py            ← CRUD de alimentos
    ├── reservations.py     ← reserva com Lock + UNIQUE INDEX
    ├── notifications.py    ← Pool.map (lote) + Queue (assíncrono)
    ├── workers.py          ← Process daemon (ExpiryChecker + NotifWorker)
    ├── simulation.py       ← 5 cenários de demonstração
    ├── benchmark.py        ← detecção de CPUs/threads + métricas
    └── api_server.py       ← ThreadingHTTPServer para workers Docker
```

---

## 1 — Banco de Dados com 1000 Usuários

### O que é o seed

O banco SQLite começa vazio. O script `seed_database.py` popula com **1000 usuários** usando `multiprocessing.Pool` para paralelizar o hashing bcrypt — operação CPU-bound.

```
50 restaurantes → restaurante01@connectongs.dev ... restaurante50@connectongs.dev
950 ONGs        → ong0001@connectongs.dev        ... ong0950@connectongs.dev
200 alimentos   → 4 por restaurante, validades aleatórias
Senha única     → Teste@1234
```

### Por que Pool para o seed?

| Método | Tempo para 1000 hashes |
|---|---|
| Serial (1 processo, rounds=12) | ~5 minutos |
| Pool(2, rounds=12) | ~2.5 minutos |
| Pool(2, rounds=4)  | **~1 segundo** |

`rounds=4` é suficiente para testes — o `bcrypt.checkpw()` continua funcionando pois os rounds ficam gravados dentro do próprio hash.

### Comandos do seed

```bash
# Criar 1000 usuários (execução inicial)
python3 seed_database.py

# Ver quantos usuários existem no banco
python3 seed_database.py --status

# Apagar tudo e recriar do zero
python3 seed_database.py --reset
```

### Saída esperada

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
python3 main.py
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

### Flags de linha de comando

```bash
python3 main.py --auto        # roda todos os cenários sem menu
python3 main.py --benchmark   # executa somente o benchmark
python3 main.py --sysinfo     # mostra informações do sistema e sai
```

---

## 3 — Mecanismos de Paralelismo (5 Cenários)

### Cenário 1 — Logins Simultâneos (`Semaphore`)

**Arquivo:** `connectongs/auth.py`

10 ONGs tentam logar ao mesmo tempo. O `Semaphore(5)` funciona como uma catraca: só 5 processos entram simultaneamente, os demais aguardam na fila.

```
[PID 1001] aguardando slot... → ADQUIRIDO → bcrypt.checkpw() → LIBERADO
[PID 1002] aguardando slot... → ADQUIRIDO → bcrypt.checkpw() → LIBERADO
[PID 1003] aguardando... (semáforo cheio, espera vaga)
```

**Por que usar:** simula throttle de autenticação sob carga — evita que bcrypt sobrecarregue o banco com 1000 logins simultâneos.

---

### Cenário 2 — Race Condition (`Lock` + `UNIQUE INDEX`)

**Arquivo:** `connectongs/reservations.py`

8 ONGs tentam reservar o **mesmo** alimento ao mesmo tempo. Apenas 1 vence; as 7 restantes recebem `409 Conflict`.

**Dupla proteção:**

```
Nível 1 — Lock (aplicação):
  ONG-A adquire Lock → verifica DISPONIVEL → insere → muda status → libera
  ONG-B aguarda Lock → vê RESERVADO → retorna 409

Nível 2 — UNIQUE INDEX (banco):
  Se dois containers Docker (sem memória compartilhada) chegarem ao mesmo tempo,
  o banco rejeita o segundo com IntegrityError → 409 Conflict
```

---

### Cenário 3 — Notificações em Lote (`Pool.map`)

**Arquivo:** `connectongs/notifications.py`

Restaurante cadastra alimento → sistema notifica todas as ONGs da região **em paralelo** via `Pool.map()`.

```
Pool(N workers)
  ├─ Worker PID-A → ONG 01 (simultâneo)
  ├─ Worker PID-B → ONG 02 (simultâneo)
  └─ Worker PID-C → ONG 03 (simultâneo)
```

---

### Cenário 4 — Fila + Worker Assíncrono (`Queue` + `Process`)

**Arquivos:** `connectongs/notifications.py`, `connectongs/workers.py`

O fluxo principal **nunca bloqueia**: coloca evento na fila e continua. O `NotificationWorker` (processo separado) consome a fila em background.

```
Fluxo principal             NotificationWorker (Process daemon)
      │                                │
  enqueue(msg) ──→ Queue ──────────→  consome → persiste no banco
      │                                │
  continua imediatamente            (em background, PID diferente)
```

---

### Cenário 5 — Expiração Automática (`Process` daemon)

**Arquivo:** `connectongs/workers.py`

O `ExpiryChecker` roda em loop independente, verificando alimentos vencidos a cada N segundos — sem depender de nenhuma requisição.

```
Loop a cada 2s:
  Ciclo 1 → 2 alimentos expirados → notifica restaurante
  Ciclo 2 → 0 expirados → aguarda
  stop_event sinalizado → encerra graciosamente
```

---

## 4 — Detecção de Threads do Sistema

```bash
python3 main.py --sysinfo
```

```
  Hostname              : codespaces-xxxxx
  Plataforma            : Linux-6.8.0-azure-x86_64
  Núcleos físicos (CPU) : 1
  Threads lógicas       : 2
  Hyperthreading        : ATIVO (2× por núcleo)
  Paralelismo máximo    : 2 processos simultâneos
  Speedup teórico (max) : 2.0x sobre execução serial
```

**Como funciona:** lê `/proc/cpuinfo` linha a linha, mapeando pares `(physical id, core id)` únicos para distinguir núcleos físicos de threads lógicas (hyperthreading).

---

## 5 — Benchmark Local: Serial vs Concorrente

```bash
python3 benchmark_runner.py
```

Executa 10 logins em três modos e compara:

```
  Modo                            Workers  Ops  Tempo(s)  Ops/s  Lat.Média(ms)  Speedup
  Serial (1 worker)                     1   10     3.907    2.6          390.7   baseline
  Concorrente (2 workers)               2   10     2.745    3.6          434.6      1.42x
  Concorrente c/ Semaphore(5)          10   10     2.015    5.0         1397.1      1.94x

  Melhor modo  : Concorrente c/ Semaphore(5)
  Speedup real : 1.94x  (teórico máximo: 2.0x com 2 CPUs)
  Eficiência   : 19.4% por worker
```

**Lei de Amdahl** — com 5% de código serial, speedup máximo por número de processadores:

```
  1 processadores → speedup teórico: 1.00x
  2 processadores → speedup teórico: 1.90x
  4 processadores → speedup teórico: 3.48x
  8 processadores → speedup teórico: 5.93x
```

---

## 6 — Benchmark Distribuído (Docker)

### Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│                     docker compose up                        │
│                                                              │
│  [seed]  ──→  cria 1000 usuários → sai                      │
│                    ↓                                         │
│  [api]   ──→  ThreadingHTTPServer :8080                      │
│               Semaphore(5) + Lock + SQLite WAL               │
│                    ↑                                         │
│  [worker_1] serial       ──┐                                 │
│  [worker_2] serial       ──┤──→ POST /auth/login × N        │
│  [worker_3] concurrent   ──┤    (cada worker usa sua fatia   │
│  [worker_4] concurrent   ──┘     dos 1000 usuários)          │
└──────────────────────────────────────────────────────────────┘
```

### Como configurar o número de requisições

Edite o arquivo **`.env`** na raiz do projeto:

```env
# Número TOTAL de requisições divididas pelos 4 workers
# Mínimo: 4  |  Máximo: 1000
TOTAL_REQUESTS=100
```

Exemplos de configuração:

| `TOTAL_REQUESTS` | Por worker (≈) | Tempo estimado |
|---|---|---|
| `40` | 10 por worker | ~10 segundos |
| `100` | 25 por worker | ~25 segundos |
| `400` | 100 por worker | ~1.5 minutos |
| `1000` | 250 por worker | ~4 minutos |

> **Nota:** o limite máximo é 1000 porque o banco tem 950 ONGs. Valores acima de 950 fazem os workers ciclar pelos mesmos usuários.

### Como rodar

```bash
# 1. Configure o número de requisições no .env
echo "TOTAL_REQUESTS=200" > .env

# 2. Execute (seed + api + 4 workers)
docker compose up --build

# 3. Acompanhe em tempo real
docker compose logs -f

# 4. Ver relatório de um worker específico
docker compose logs worker_1

# 5. Salvar todos os logs
docker compose logs --no-color > resultado.txt

# 6. Encerrar e limpar volumes (para próxima execução limpa)
docker compose down -v
```

### Saída esperada dos workers

```
════════════════════════════════════════════════════════════
  CONNECTONGS Worker-1/4 | PID 7
  API     : http://api:8080
  Modo    : serial | Reqs: 25 (total=100 ÷ 4 workers)
════════════════════════════════════════════════════════════

[Worker-1] API disponível (PID=1, uptime=6.4s)
[Worker-1] 237 ONGs atribuídas (offset=0)
[Worker-1] === SERIAL === 25 requisições de login

[Worker-1] OK   Req   10/25  lat=   81ms  throughput=3.1 req/s
[Worker-1] OK   Req   20/25  lat=   65ms  throughput=3.4 req/s
[Worker-1] OK   Req   25/25  lat=   72ms  throughput=3.3 req/s

════════════════════════════════════════════════════════════
  RELATÓRIO FINAL — Worker-1 | Modo: SERIAL
────────────────────────────────────────────────────────────
  Requisições  : 25
  Throughput   : 3.3 req/s
  Latência Avg : 90.3 ms
  Latência Min : 56.8 ms
  Latência Max : 143.5 ms
  Latência p50 : 62.9 ms
  Latência p95 : 131.2 ms
  Latência p99 : 141.8 ms
════════════════════════════════════════════════════════════
```

### Resultados obtidos (950 ONGs reais, banco com rounds=4)

| Container | Modo | Reqs | Throughput | Lat. Avg | Lat. p99 |
|---|---|---|---|---|---|
| worker_1 | Serial | 237 | 11.1 req/s | 90 ms | 654 ms |
| worker_2 | Serial | 237 | 11.7 req/s | 86 ms | 308 ms |
| worker_3 | Concorrente (5 threads) | 237 | 5.9 req/s | 169 ms | 362 ms |
| worker_4 | Concorrente (10 threads) | 239 | 4.3 req/s | 231 ms | 354 ms |
| **Total distribuído** | — | **950** | **~33 req/s** | — | — |

**Comparação com execução serial pura (1 processo, sem Docker):** 2.6 req/s
**Ganho com distribuição:** ~12.7× mais throughput

**Por que serial por worker supera concorrente por worker?**
O servidor tem 2 CPUs e bcrypt satura CPU. Quando worker_4 abre 10 threads, todas competem pelos mesmos 2 núcleos do servidor — o Semaphore(5) cria fila, aumentando latência. O worker serial envia uma requisição de cada vez, e o servidor processa sem espera de fila.

---

## 7 — API HTTP (para distribuição)

**Arquivo:** `connectongs/api_server.py`

Usa `ThreadingHTTPServer` da biblioteca padrão (sem Flask, sem FastAPI). Cada requisição é tratada em uma thread separada.

### Endpoints

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/health` | Status do servidor (pid, uptime) |
| `GET` | `/stats` | Contagem de registros no banco |
| `GET` | `/users/ongs?limit=N&offset=O` | Paginação de ONGs para os workers |
| `GET` | `/users/restaurants` | Lista restaurantes |
| `GET` | `/foods/available` | Alimentos disponíveis para reserva |
| `POST` | `/auth/register` | Cadastro de usuário |
| `POST` | `/auth/login` | Login com Semaphore (throttle) |
| `POST` | `/foods/add` | Cadastrar alimento |
| `POST` | `/foods/reserve` | Reservar alimento com Lock |

### Rodar o servidor localmente (sem Docker)

```bash
# Terminal 1 — inicia o servidor na porta 8080
python3 -m connectongs.api_server

# Terminal 2 — testa um endpoint
curl http://localhost:8080/health
curl http://localhost:8080/stats
curl http://localhost:8080/users/ongs?limit=5
```

### Exemplo de chamada de login via curl

```bash
curl -X POST http://localhost:8080/auth/login \
     -H "Content-Type: application/json" \
     -d '{"email":"ong0001@connectongs.dev","password":"Teste@1234"}'

# Resposta:
# {"success": true, "user_id": 2, "user_type": "ONG", "elapsed_ms": 85.3}
```

---

## 8 — Banco de Dados (SQLite WAL)

**Arquivo:** `connectongs/database.py`

### Por que SQLite aguenta múltiplos processos?

```sql
PRAGMA journal_mode=WAL;      -- múltiplos leitores simultâneos com 1 escritor
PRAGMA busy_timeout=10000;    -- espera até 10s por lock em vez de falhar
PRAGMA foreign_keys=ON;       -- integridade referencial ativa
```

O modo WAL (Write-Ahead Log) permite que leituras e escritas aconteçam ao mesmo tempo. Sem WAL, qualquer `INSERT` bloquearia todas as `SELECT`.

### Schema

| Tabela | Colunas principais | Propósito |
|---|---|---|
| `users` | id, name, email (UNIQUE), password_hash, user_type, region | Restaurantes e ONGs |
| `foods` | id, restaurant_id, name, category, quantity, expiry_date, status | Alimentos para doação |
| `reservations` | id, food_id, ong_id, status | Reservas (1 ativa por alimento) |
| `notifications` | id, user_id, message, read | Mensagens enviadas |
| `audit_log` | id, event_type, user_id, details, process_id | Rastreabilidade (LGPD) |

**Índice mais importante:**
```sql
CREATE UNIQUE INDEX idx_unique_active_reservation
    ON reservations(food_id)
    WHERE status != 'CANCELADO';
```
Garante que nunca existam duas reservas ativas para o mesmo alimento — mesmo em ambiente distribuído, onde o `Lock` de memória não funciona.

---

## 9 — Fluxo Completo de uma Reserva

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
NotificationWorker (Process separado) persiste no banco
```

---

## 10 — Evidências de Paralelismo na Saída

Ao rodar qualquer cenário, os logs mostram PIDs diferentes operando ao mesmo tempo:

```
[13:56:44.456] [PID 18633] [SUCCESS] Login OK → ONG Solidária 01 [PID 18633]
[13:56:44.458] [PID 18634] [SUCCESS] Login OK → ONG Solidária 03 [PID 18634]
[13:56:44.837] [PID 18633] [SUCCESS] Login OK → ONG Solidária 02 [PID 18633]
```

Dois processos com PIDs diferentes (`18633` e `18634`) executando **no mesmo milissegundo** — isso é paralelismo real, não concorrência simulada.

---

## 11 — Variáveis de Ambiente

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

## 12 — Dependências

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
| `os`, `platform` | Detecção de CPUs via /proc/cpuinfo |

---

## Referência Rápida de Comandos

```bash
# ── Setup inicial ──────────────────────────────────────────
pip install -r requirements.txt       # instala bcrypt
python3 seed_database.py              # cria 1000 usuários

# ── Simulações de paralelismo ──────────────────────────────
python3 main.py                       # menu interativo
python3 main.py --auto                # todos os 5 cenários

# ── Informações do sistema ─────────────────────────────────
python3 main.py --sysinfo             # CPUs e threads

# ── Benchmark local ────────────────────────────────────────
python3 benchmark_runner.py           # serial vs concorrente
python3 main.py --benchmark           # mesmo benchmark via menu

# ── API server (para testes locais) ───────────────────────
python3 -m connectongs.api_server     # sobe na porta 8080

# ── Benchmark distribuído (Docker) ────────────────────────
nano .env                             # edita TOTAL_REQUESTS
docker compose up --build             # roda tudo
docker compose logs -f                # acompanha em tempo real
docker compose logs --no-color > resultado.txt
docker compose down -v                # limpa tudo

# ── Banco de dados ─────────────────────────────────────────
python3 seed_database.py --status     # quantos usuários existem
python3 seed_database.py --reset      # apaga e recria tudo
```
