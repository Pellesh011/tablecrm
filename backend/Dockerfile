FROM python:3.9-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /backend

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    wkhtmltopdf \
    xvfb \
    libfontconfig \
    libjpeg62-turbo \
    libxrender1 \
    tesseract-ocr \
    tesseract-ocr-rus \
    poppler-utils \
    build-essential \
    libssl-dev \
    libffi-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r appuser -g 1000 && useradd -r -g appuser appuser -u 1000

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

RUN mkdir tmp && \
    chown -R appuser:appuser /backend

COPY --chown=appuser:appuser . .

# TODO: запускать от non-root пользователя
# после того, как поправим права в монтируемых
# в контейнер томах (/photos) (может отказаться от bind монтирования в пользу volumes)
# USER appuser

ENTRYPOINT ["/usr/bin/tini", "--", "docker-entrypoint.sh"]

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info", "--workers", "4"]
