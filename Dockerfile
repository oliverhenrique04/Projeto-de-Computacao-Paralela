# ────────────────────────────────────────────────────────────────────────────
# CONNECTONGS — Imagem Docker
# Computação Paralela e Distribuída — Fase 2
#
# Usos:
#   API Server  : docker run -e ROLE=api   -p 8080:8080 connectongs
#   Worker      : docker run -e ROLE=worker -e API_URL=http://api:8080 connectongs
#
# Via Docker Compose: docker compose up --build
# ────────────────────────────────────────────────────────────────────────────

FROM python:3.12-slim

LABEL maintainer="CONNECTONGS — Computação Paralela e Distribuída"
LABEL description="Sistema de doação de alimentos com multiprocessing e distribuição"

# Diretório de trabalho dentro do container
WORKDIR /app

# Instala dependências Python (apenas bcrypt — o resto é stdlib)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código-fonte do projeto
COPY connectongs/ ./connectongs/
COPY worker_client.py .
COPY main.py .
COPY benchmark_runner.py .
COPY seed_database.py .

# Cria diretório para o banco SQLite (montado como volume no Compose)
RUN mkdir -p /data

# Variáveis de ambiente padrão
ENV CONNECTONGS_DB=/data/connectongs.db
ENV API_PORT=8080
ENV API_HOST=0.0.0.0
ENV API_URL=http://api:8080
ENV WORKER_ID=1
ENV N_REQUESTS=10
ENV MODE=serial
ENV CONCURRENT_N=3
ENV ROLE=api

# Expõe a porta do API server
EXPOSE 8080

# Script de entrada que decide o papel do container via ROLE
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
