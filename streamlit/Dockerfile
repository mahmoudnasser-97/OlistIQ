# =============================================================================
# streamlit/Dockerfile
#
# Base  : python:3.11-slim
# Runs  : Streamlit real-time operations dashboard
#         Reads live order metrics from Redis
#
# Access: http://localhost:8501
# =============================================================================

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# OS packages
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Python packages
RUN pip install --no-cache-dir \
    streamlit==1.30.0 \
    redis==5.0.1 \
    pandas==2.0.3 \
    plotly==5.17.0 \
    psycopg2-binary==2.9.9

# App source
COPY . /app/

# Streamlit config
RUN mkdir -p /root/.streamlit \
    && cat > /root/.streamlit/config.toml << 'EOF'
[server]
headless                = true
address                 = "0.0.0.0"
port                    = 8501
enableCORS              = false
enableXsrfProtection    = false

[browser]
gatherUsageStats        = false

[theme]
base                    = "dark"
primaryColor            = "#4CAF50"
EOF

EXPOSE 8501

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "dashboard.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0"]
