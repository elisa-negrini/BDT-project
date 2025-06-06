FROM python:3.11-slim

WORKDIR /app

# Copia requirements dalla cartella locale
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copia lo script Streamlit
COPY dashboard.py .

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]