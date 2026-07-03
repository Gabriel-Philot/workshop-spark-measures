ARG LAB7_DASHBOARD_BASE_IMAGE=python:3.12-slim-bookworm
FROM ${LAB7_DASHBOARD_BASE_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
ENV DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions

WORKDIR /app

COPY context/python-wheels /tmp/python-wheels
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-index --find-links=/tmp/python-wheels -r /tmp/requirements.txt \
    && mkdir -p /opt/duckdb/extensions \
    && python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs'); con.execute('INSTALL delta'); con.close()" \
    && rm -rf /tmp/python-wheels /tmp/requirements.txt /root/.cache

COPY context/dashboard /app

EXPOSE 8501

CMD ["streamlit", "run", "/app/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
