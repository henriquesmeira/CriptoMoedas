FROM python:3.9

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install dbt-core dbt-postgres  # Troca pelo adaptador que precisares

CMD ["tail", "-f", "/dev/null"]
