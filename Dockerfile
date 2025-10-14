FROM python:3.11.10-slim-bookworm

# Atualiza os pacotes do sistema para garantir segurança
RUN apt-get update && apt-get upgrade -y

# Define a pasta de trabalho dentro do container
WORKDIR /app

# Copia a lista de bibliotecas e as instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo projeto
COPY . .

# Comando que será executado por padrão
CMD ["python", "Etl_yahoofinance/etl_finance.py"]