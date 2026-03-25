# Define a imagem base do Python, versão slim para ser mais leve
FROM python:3.11-slim

# Impede a criação de arquivos de cache do Python (.pyc) e define output direto sem buffer
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Define a porta padrão do container (o app.py usa a porta 3000 como fallback)
ENV PORT=3000

# Diretório de trabalho dentro do container
WORKDIR /app/backend

# Atualiza os pacotes e instala dependências básicas do Linux, se necessário pelo pandas/numpy
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia os requirements primeiro para usar o cache do Docker durante a instalação
COPY requirements.txt .

# Instala as dependências do backend
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código do backend para o container
COPY . .

# Expõe a porta
EXPOSE 3000

# Configura o Gunicorn para iniciar o app do Flask
# Utiliza o app:app por referenciar o arquivo app.py e a variável 'app' correspondente à instância do Flask
CMD gunicorn --bind 0.0.0.0:$PORT --workers 4 --threads 4 --timeout 600 app:app
