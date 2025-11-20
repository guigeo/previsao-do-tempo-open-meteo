# Imagem base oficial do Python, versão leve
FROM python:3.11-slim

# Evita criação de .pyc e melhora logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Define a pasta de trabalho dentro do container
WORKDIR /app

# Copia somente o requirements primeiro (melhora cache de build)
COPY requirements.txt .

# Instala as dependências do projeto
RUN pip install --no-cache-dir -r requirements.txt

# Agora copia TODO o código do projeto para dentro da imagem
COPY . .

# Comando padrão quando o container subir
# Você pode trocar "ambos" por "diario" ou "horario" se quiser
CMD ["python", "main.py", "--modo", "ambos"]
