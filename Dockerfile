FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bot_server.py setup_telegram_webhook.py .
CMD ["sh", "-c", "if [ -n \"$PUBLIC_BASE_URL\" ]; then python setup_telegram_webhook.py \"$PUBLIC_BASE_URL\" || echo 'Telegram webhook registration unsuccessful'; fi; exec uvicorn bot_server:app --host 0.0.0.0 --port 8000 --workers 1"]
