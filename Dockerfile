# Imagem base oficial do Python + uv (gerenciador de dependências)
FROM python:3.11-slim

# Copia os binários do uv da imagem oficial
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Evita .pyc, melhora logs e compila bytecode na instalação
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# Instala as dependências primeiro (melhora cache de build)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copia o restante do código
COPY . .

# Coloca o venv do projeto no PATH (entrypoint usa o python do venv)
ENV PATH="/app/.venv/bin:$PATH"

# Comando padrão: troque "ambos" por "diario"/"horario" se quiser
CMD ["python", "main.py", "--modo", "ambos"]
