CONNECTONGS — Computação Paralela (Fase 2)
Estrutura de arquivos

connectongs/
├── logger.py         # Log colorido com timestamp e PID
├── database.py       # SQLite WAL + UNIQUE INDEX anti-race
├── auth.py           # Registro e login
├── foods.py          # CRUD de alimentos
├── reservations.py   # Reserva com dupla proteção
├── notifications.py  # Queue + Pool
├── workers.py        # Processos daemon
└── simulation.py     # 5 cenários de demo
main.py               # Entry point
requirements.txt      # bcrypt apenas
5 mecanismos de paralelismo implementados
Cenário	Mecanismo	O que demonstra
Login simultâneo	Semaphore(5) + Pool(10)	Throttle de 10 logins, max 5 em paralelo
Corrida por alimento	Lock + UNIQUE INDEX	8 ONGs disputam 1 alimento — só 1 vence (409 Conflict)
Notificações em lote	Pool.map	N workers enviam para N ONGs simultaneamente
Fila assíncrona	Queue + Process	Worker daemon processa em background sem bloquear o fluxo
Expiração automática	Process daemon	Verifica e expira alimentos em loop independente
Como rodar

pip install -r requirements.txt

# Menu interativo
python3 main.py

# Demo automático completo (para apresentação)
python3 main.py --auto
Para o dev de distribuição Docker
CONNECTONGS_DB — aponta o banco para outro path/volume
Os workers (workers.py) são processos independentes prontos para virar containers separados
A fila (multiprocessing.Queue) pode ser trocada por Redis/RabbitMQ sem alterar a lógica de negócio