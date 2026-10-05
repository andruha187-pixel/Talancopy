FROM python:3.11-slim

WORKDIR /app

# build-essential — на случай, если для какой-то зависимости (eth-*/coincurve)
# нет готового колеса под платформу сервера.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
# Фоновый воркер, HTTP-порта нет — в Coolify отключи healthcheck по порту.
CMD ["python", "-m", "src.main"]
