FROM astrocrpublic.azurecr.io/runtime:3.0-4

RUN mkdir -p /usr/local/airflow/include/data/raw \
    /usr/local/airflow/include/data/extracted \
    /usr/local/airflow/include/data/archive && \
    chmod -R 777 /usr/local/airflow/include/data && \
    chown -R astro:0 /usr/local/airflow/include