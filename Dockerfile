FROM python:3.9

WORKDIR /app

# Instala dependências básicas + driver do banco de dados
RUN pip install --upgrade pip && \
    pip install psycopg2-binary sqlalchemy pandas

COPY . .

CMD ["python", "main.py"]
