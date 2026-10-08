FROM python:3.12-slim

RUN pip install --no-cache-dir psycopg2-binary==2.9.10

WORKDIR /app
COPY app.py index.html ./

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=4)"

CMD ["python", "app.py"]
