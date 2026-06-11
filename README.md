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

## Como executar

```bash
# 1. Instalar dependências
pip install -r requirements.txt

# 2. Menu interativo
python3 main.py

# 3. Demo automático completo (para apresentação)
python3 main.py --auto
```

Para distribuição com Docker, o outro desenvolvedor pode apontar o banco via variável de ambiente:

```bash
CONNECTONGS_DB=/data/connectongs.db python3 main.py --auto
```

---

## Mecanismos de Paralelismo Implementados

O projeto usa **exclusivamente** a biblioteca `multiprocessing` do Python — sem `async/await`, sem `threading` solto. Cada mecanismo é demonstrado em um cenário ao vivo.

| Mecanismo | Onde | O que resolve |
|---|---|---|
| `multiprocessing.Semaphore` | `auth.py` | Limita logins simultâneos |
| `multiprocessing.Lock` | `reservations.py` | Evita dupla reserva do mesmo alimento |
| `multiprocessing.Pool` | `notifications.py` | Dispara notificações em paralelo |
| `multiprocessing.Queue` | `notifications.py` | Fila assíncrona de notificações |
| `multiprocessing.Process` | `workers.py` | Workers daemon desacoplados |
| SQLite `UNIQUE INDEX` | `database.py` | Garantia final do banco contra race condition |

---

## Estrutura de Arquivos

```
connectongs/
├── logger.py
├── database.py
├── auth.py
├── foods.py
├── reservations.py
├── notifications.py
├── workers.py
└── simulation.py
main.py
requirements.txt
```

---

## Descrição de Cada Arquivo

### `main.py` — Ponto de Entrada

Arquivo que inicia o sistema. Contém o guard `if __name__ == '__main__'` obrigatório para o `multiprocessing` funcionar corretamente no Windows e macOS (no Linux é bom costume).

Oferece dois modos:
- **Menu interativo**: o usuário escolhe qual cenário rodar
- **`--auto`**: roda todos os 5 cenários em sequência (ideal para demo e apresentação)

Não contém lógica de negócio — apenas orquestra os módulos.

---

### `connectongs/logger.py` — Log Colorido com PID

Fornece funções de log (`info`, `success`, `warning`, `error`, `worker`, `conflict`, `lock_log`, `sim`) que imprimem mensagens coloridas no terminal com:
- **Timestamp** em milissegundos (`HH:MM:SS.mmm`)
- **PID do processo** que gerou a mensagem

O PID é a evidência visual de que processos diferentes estão rodando em paralelo. Quando você vê `[PID 34573]` e `[PID 34580]` na mesma linha de tempo, está vendo paralelismo real.

---

### `connectongs/database.py` — Banco de Dados

Gerencia o SQLite com as seguintes características para suportar múltiplos processos simultâneos:

- **WAL mode** (`PRAGMA journal_mode=WAL`): permite múltiplos processos lendo ao mesmo tempo enquanto outro escreve. Sem WAL, qualquer escrita bloquearia todas as leituras.
- **`busy_timeout=10000`**: se o banco estiver bloqueado por outro processo, espera até 10 segundos antes de falhar (evita erro imediato em alta concorrência).
- **Cada processo abre sua própria conexão**: conexões SQLite não são seguras para compartilhar entre processos. O padrão `get_conn()` garante que cada processo crie a sua.
- **`UNIQUE INDEX idx_unique_active_reservation`**: a restrição mais importante do sistema. Garante no banco de dados que o mesmo alimento não pode ter duas reservas ativas ao mesmo tempo. É a última linha de defesa — funciona mesmo em ambiente distribuído com containers separados.
- **Tabela `audit_log`**: registra todos os eventos relevantes com o PID de quem os gerou, atendendo ao requisito de trilha de auditoria (LGPD).

**Tabelas criadas:**

| Tabela | Propósito |
|---|---|
| `users` | Restaurantes, ONGs e Administradores |
| `foods` | Alimentos cadastrados para doação |
| `reservations` | Conexões ONG ↔ Alimento |
| `notifications` | Mensagens enviadas aos usuários |
| `audit_log` | Trilha de auditoria de todos os eventos |

---

### `connectongs/auth.py` — Autenticação com Semáforo

Responsável por cadastro e login de usuários.

**`register()`**: hash da senha com `bcrypt` antes de salvar. O bcrypt é intencionalmente lento — isso é uma característica de segurança, não um bug.

**`login()` com `multiprocessing.Semaphore`**:

```
[PID 34541] aguardando slot... → ADQUIRIDO → faz login → LIBERADO
[PID 34542] aguardando slot... → ADQUIRIDO → faz login → LIBERADO
[PID 34543] aguardando slot... (bloqueado, semáforo cheio)
                                             ↑ só entra quando outro sair
```

O `Semaphore(5)` funciona como uma "catraca" com 5 torniquetes. Se 5 processos já estão autenticando, o 6° espera na fila até um dos 5 terminar. Isso simula throttle de autenticação sob alta carga — padrão usado em sistemas reais para evitar sobrecarga do banco por bcrypt.

O semáforo é criado no processo principal e passado para os workers via `initializer` do `Pool`, garantindo que todos compartilhem o mesmo objeto de sincronização.

---

### `connectongs/foods.py` — Gestão de Alimentos

CRUD de alimentos. Destaque para duas funções:

**`add_food()`**: valida que a data de validade é futura (Regra de Negócio RN01 do documento). Lança `ValueError` se a data já passou.

**`expire_foods()`**: chamada pelo `ExpiryChecker` em background. Executa um `UPDATE` em lote marcando todos os alimentos com `expiry_date < hoje` como `EXPIRADO`. Retorna a quantidade afetada para que o worker possa disparar notificações aos restaurantes correspondentes.

Esta função é **desacoplada** — não é chamada por nenhum fluxo de requisição. Apenas o worker de expiração a chama, de forma independente.

---

### `connectongs/reservations.py` — Reserva com Dupla Proteção

O módulo mais crítico do sistema. Implementa proteção em dois níveis contra a race condition de "duas ONGs reservando o mesmo alimento":

**Nível 1 — `multiprocessing.Lock` (aplicação)**:

```
ONG-A adquire Lock → verifica disponível → insere → atualiza status → libera Lock
                      ONG-B aguarda Lock
                                          ONG-B adquire Lock → vê RESERVADO → retorna 409
```

O Lock garante que a sequência "verificar disponibilidade + inserir reserva + atualizar status" seja **atômica** — nenhum outro processo entra no meio. É a proteção principal.

**Nível 2 — `UNIQUE INDEX` no banco (dados)**:

Se dois processos em containers Docker diferentes (sem memória compartilhada) tentarem inserir ao mesmo tempo, o banco rejeita a segunda inserção com `IntegrityError`. O código captura essa exceção e retorna 409 Conflict.

> Esta dupla proteção é o padrão correto para sistemas distribuídos: o Lock resolve dentro do mesmo servidor; o índice único resolve entre servidores diferentes.

---

### `connectongs/notifications.py` — Dois Padrões de Paralelismo

Implementa **duas estratégias** diferentes para envio de notificações:

#### Padrão 1: `Pool.map` — Dispatch em Lote

Usado quando um restaurante cadastra um alimento novo e precisa notificar todas as ONGs da região ao mesmo tempo.

```
Pool(N workers)
  ├─ Worker PID-A → envia para ONG 01
  ├─ Worker PID-B → envia para ONG 02
  ├─ Worker PID-C → envia para ONG 03
  └─ Worker PID-D → envia para ONG 04
         (todos ao mesmo tempo)
```

`dispatch_bulk(ong_ids, message)` cria um Pool com `min(N, cpu_count)` workers e distribui as ONGs entre eles via `pool.map()`. O processo principal só continua quando todas as notificações foram entregues.

#### Padrão 2: `Queue` + `Process` — Fila Assíncrona

Usado para eventos de sistema (confirmações, lembretes, alertas) onde o fluxo principal **não pode esperar** o envio terminar.

```
Fluxo principal          NotificationWorker (processo separado)
      │                           │
  enqueue(msg) ──→ Queue ──→  consome msg → persiste no banco
      │                           │
  continua...                  (em background)
```

`enqueue(user_id, message)` coloca a mensagem na fila e retorna imediatamente. O `NotificationWorker` (rodando em um `Process` separado, iniciado pelo `workers.py`) consome a fila em loop e persiste cada notificação no banco.

> Esta é a arquitetura "fila + worker" que o critério de avaliação exige como prova de paralelismo real.

---

### `connectongs/workers.py` — Processos Daemon em Background

Fábrica de processos de background que rodam de forma totalmente independente do fluxo principal.

**`expiry_checker_loop()`**: loop infinito que, a cada N segundos:
1. Chama `foods.expire_foods()` para marcar alimentos vencidos
2. Busca os restaurantes afetados
3. Enfileira notificações para eles via `notifications.enqueue()`
4. Aguarda o próximo ciclo com `stop_event.wait(timeout=interval)`

O uso de `stop_event.wait()` ao invés de `time.sleep()` é importante: permite que o processo encerre imediatamente quando o evento for sinalizado, em vez de aguardar o timeout completo.

**`start_expiry_checker()`** e **`start_notification_worker()`**: funções factory que criam, configuram e iniciam os processos, retornando o objeto `Process` para que o chamador possa fazer `join()` no encerramento.

Ambos os processos são marcados como `daemon=True` — se o processo principal morrer, eles morrem junto, sem deixar processos órfãos.

---

### `connectongs/simulation.py` — Cenários de Demonstração

Contém os 5 cenários que provam os mecanismos funcionando ao vivo.

> As funções `_login_worker` e `_reserve_worker` estão no **nível do módulo** (não são closures). Isso é obrigatório: o `multiprocessing.Pool` precisa serializar (pickle) as funções para enviar aos workers, e closures não são serializáveis.

| Função | Cenário |
|---|---|
| `cenario_logins_simultaneos()` | Pool de 10 processos + Semaphore(5) |
| `cenario_corrida_alimento()` | Pool de 8 processos + Lock + 1 alimento |
| `cenario_notificacoes_lote()` | Pool.map para N ONGs em paralelo |
| `cenario_fila_notificacoes()` | Queue + Process daemon |
| `cenario_expiracao_automatica()` | Process daemon em loop com intervalo |
| `print_report()` | Relatório final do estado do banco |

`seed_users()` é **idempotente**: pode ser chamada múltiplas vezes sem duplicar dados. Se o usuário já existe (email único), reutiliza o registro existente. Isso permite re-executar o demo sem resetar o banco.

---

## Fluxo Completo de uma Reserva

```
ONG faz login
    │
    ▼
Semaphore(5) ─── max 5 logins simultâneos
    │
    ▼
ONG busca alimentos disponíveis (list_foods)
    │
    ▼
ONG clica "Reservar"
    │
    ▼
Lock.acquire() ─── seção crítica começa
    │
    ├─ verifica: food.status == 'DISPONIVEL'?
    │     NÃO → retorna 409 Conflict
    │     SIM ↓
    ├─ INSERT INTO reservations (food_id, ong_id)
    │     UNIQUE INDEX violado? → retorna 409 Conflict
    │     OK ↓
    ├─ UPDATE foods SET status='RESERVADO'
    │
Lock.release() ─── seção crítica termina
    │
    ▼
Restaurante recebe notificação via Queue → NotificationWorker → banco
```

---

## Evidências de Paralelismo (saída do terminal)

Na saída do `--auto` é possível observar:

- PIDs diferentes operando ao mesmo tempo (ex: `[PID 34541]` e `[PID 34580]` na mesma fração de segundo)
- Semáforo: 5 processos adquirem o slot enquanto os outros 5 ficam em `aguardando`
- Race condition: `ONG Solidária 01` vence, as outras 7 recebem `CONFLITO 409`
- Fila assíncrona: `[Main PID]` enfileira e continua; `[NotifWorker PID]` processa separadamente
- ExpiryChecker: `Ciclo 1 — 2 expirados`, `Ciclo 2 — nenhum`, encerra graciosamente

---

## Preparação para Distribuição Docker

O código foi escrito para facilitar a separação em containers pelo outro desenvolvedor:

- **`CONNECTONGS_DB`**: variável de ambiente para apontar o banco para um volume compartilhado ou outro host
- **Workers independentes**: `expiry_checker_loop` e `notification_worker_loop` são funções puras que recebem seus parâmetros — podem ser movidas para um `Dockerfile` separado sem alterar a lógica
- **Fila substituível**: `notifications.set_notification_queue(q)` aceita qualquer objeto com interface `.put()` — pode ser trocado por um wrapper de Redis sem mudar o restante do código

---

## Dependências

```
bcrypt==4.2.1
```

Todo o resto é biblioteca padrão do Python 3.8+:
`multiprocessing`, `sqlite3`, `datetime`, `time`, `os`, `sys`
