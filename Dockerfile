FROM python:3.11-slim

WORKDIR /app

# Системные зависимости для сборки некоторых eth-* пакетов (py-clob-client
# тянет за собой криптографию с C-расширениями).
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Никакого HTTP-сервера у бота нет — это фоновый воркер. Если платформа
# (Coolify/Render/etc.) настаивает на healthcheck по порту, отключи
# healthcheck в настройках приложения, а не открывай тут порт искусственно.
CMD ["python", "-m", "src.main"]
