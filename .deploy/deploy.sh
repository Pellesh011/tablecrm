#!/bin/env bash

set -eu

SERVICE_NAME="backend"
IMAGE_NAME="git.tablecrm.com:5050/tablecrm/tablecrm/backend:$CI_COMMIT_SHA"
NGINX_CONTAINER_NAME="nginx"
UPSTREAM_CONF_PATH="/etc/nginx/dir/upstream.conf"
BOT_SERVICE_NAME="telegram_bot"
BOT_CONTAINER_NAME_PREFIX="telegram_bot"

reload_nginx() {
  docker exec $NGINX_CONTAINER_NAME nginx -s reload
}

update_upstream_conf() {
  NEW_PORT=$1

  new_container_id=$(docker ps -f name="${SERVICE_NAME}_$NEW_PORT" -q | head -n1)
  new_container_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{"\n"}}{{end}}' "$new_container_id" | head -n1)

  echo "Обновление upstream.conf для указания порта $NEW_PORT и ip ${new_container_ip}"

  echo "server ${new_container_ip}:8000;" > upstream.conf.tmp

  docker cp upstream.conf.tmp $NGINX_CONTAINER_NAME:$UPSTREAM_CONF_PATH

  rm upstream.conf.tmp
}

deploy_new_version() {
  current_ports=$(docker ps --filter "name=$SERVICE_NAME" --format '{{.Ports}}' | grep -o '0\.0\.0\.0:[0-9]\+' | awk -F: '{print $2}' || true)

  if [[ " $current_ports " =~ "8000" ]]; then
    NEW_PORT=8002
  else
    NEW_PORT=8000
  fi

  echo "Деплой новой версии на порт $NEW_PORT"


  docker run -d \
    --name "${SERVICE_NAME}_$NEW_PORT" \
    --restart always \
    --network infrastructure \
    -v "/certs:/certs" \
    -p "$NEW_PORT":8000 \
    -e PKPASS_PASSWORD="$PKPASS_PASSWORD" \
    -e APPLE_PASS_TYPE_ID="$APPLE_PASS_TYPE_ID" \
    -e APPLE_TEAM_ID="$APPLE_TEAM_ID" \
    -e APPLE_CERTIFICATE_PATH="$APPLE_CERTIFICATE_PATH" \
    -e APPLE_KEY_PATH="$APPLE_KEY_PATH" \
    -e APPLE_WWDR_PATH="$APPLE_WWDR_PATH" \
    -e APPLE_NOTIFICATION_PATH="$APPLE_NOTIFICATION_PATH" \
    -e APPLE_NOTIFICATION_KEY="$APPLE_NOTIFICATION_KEY" \
    -e RABBITMQ_HOST="$RABBITMQ_HOST" \
    -e RABBITMQ_PORT="$RABBITMQ_PORT" \
    -e RABBITMQ_USER="$RABBITMQ_USER" \
    -e RABBITMQ_PASS="$RABBITMQ_PASS" \
    -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
    -e APP_URL="$APP_URL" \
    -e S3_ACCESS="$S3_ACCESS" \
    -e S3_SECRET="$S3_SECRET" \
    -e S3_URL="$S3_URL" \
    -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
    -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
    -e REDIS_PASS="$REDIS_PASS" \
    -e REDIS_URL="$REDIS_URL" \
    -e TG_TOKEN="$TG_TOKEN" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASS="$POSTGRES_PASS" \
    -e POSTGRES_HOST="$POSTGRES_HOST" \
    -e POSTGRES_PORT="$POSTGRES_PORT" \
    -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
    -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
    -e ADMIN_ID="$ADMIN_ID" \
    -e YOOKASSA_OAUTH_APP_CLIENT_ID="$YOOKASSA_OAUTH_APP_CLIENT_ID" \
    -e YOOKASSA_OAUTH_APP_CLIENT_SECRET="$YOOKASSA_OAUTH_APP_CLIENT_SECRET" \
    -e RABBITMQ_USER_AMO_INTEGRATION="$RABBITMQ_USER_AMO_INTEGRATION" \
    -e RABBITMQ_PASS_AMO_INTEGRATION="$RABBITMQ_PASS_AMO_INTEGRATION" \
    -e RABBITMQ_HOST_AMO_INTEGRATION="$RABBITMQ_HOST_AMO_INTEGRATION" \
    -e RABBITMQ_PORT_AMO_INTEGRATION="$RABBITMQ_PORT_AMO_INTEGRATION" \
    -e RABBITMQ_VHOST_AMO_INTEGRATION="$RABBITMQ_VHOST_AMO_INTEGRATION" \
    -e GEOAPIFY_SECRET="$GEOAPIFY_SECRET" \
    "$IMAGE_NAME" \
    /bin/bash -c "uvicorn main:app --host=0.0.0.0 --port 8000 --log-level=info --workers 4 --timeout-keep-alive 65 --limit-max-requests 1000 --limit-concurrency 200 --backlog 128"

  for i in {1..90}; do
    if curl --silent --fail http://localhost:${NEW_PORT}/health; then
      echo "Новый сервис на порту $NEW_PORT успешно запущен"
      break
    fi
    echo "Ожидание запуска сервиса на порту $NEW_PORT..."
    sleep 1
  done

  if ! curl --silent --fail http://localhost:${NEW_PORT}/health; then
    echo "Новый сервис на порту $NEW_PORT не отвечает. Откат."
    echo "Последние логи ${SERVICE_NAME}_$NEW_PORT перед откатом:"
    docker logs --tail 200 "${SERVICE_NAME}_$NEW_PORT" || true
    docker stop "${SERVICE_NAME}_$NEW_PORT"
    docker rm "${SERVICE_NAME}_$NEW_PORT"
    exit 1
  fi

  update_upstream_conf $NEW_PORT

  reload_nginx

  sleep 30

  if [ -n "$current_ports" ]; then
    for port in $current_ports; do
      echo "Останавливаем старый сервис на порту $port"
      docker stop "${SERVICE_NAME}_${port}"
      docker rm "${SERVICE_NAME}_${port}"
    done
  fi

  echo "Деплой завершен успешно"
}

deploy_new_bot_version() {
  current_bots=$(docker ps --filter "name=${BOT_SERVICE_NAME}" --format '{{.Names}}' | grep -E "${BOT_CONTAINER_NAME_PREFIX}_[0-9]+" || true)

  if [[ -z "$current_bots" ]]; then
    NEW_BOT_NAME="${BOT_CONTAINER_NAME_PREFIX}_1"
  else
    last_bot=$(echo "$current_bots" | sort -V | tail -n1)
    last_num=$(echo "$last_bot" | grep -o '[0-9]\+$')
    NEW_NUM=$((last_num + 1))
    NEW_BOT_NAME="${BOT_CONTAINER_NAME_PREFIX}_${NEW_NUM}"
  fi

  echo "Деплой новой версии Telegram бота с именем $NEW_BOT_NAME"

  docker run -d \
    --name "$NEW_BOT_NAME" \
    --restart always \
    --network infrastructure \
    -v "/photos:/backend/photos" \
    -e RABBITMQ_HOST="$RABBITMQ_HOST" \
    -e RABBITMQ_PORT="$RABBITMQ_PORT" \
    -e RABBITMQ_USER="$RABBITMQ_USER" \
    -e RABBITMQ_PASS="$RABBITMQ_PASS" \
    -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
    -e APP_URL="$APP_URL" \
    -e S3_ACCESS="$S3_ACCESS" \
    -e S3_SECRET="$S3_SECRET" \
    -e S3_URL="$S3_URL" \
    -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
    -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
    -e TG_TOKEN="$TG_TOKEN" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASS="$POSTGRES_PASS" \
    -e POSTGRES_HOST="$POSTGRES_HOST" \
    -e POSTGRES_PORT="$POSTGRES_PORT" \
    -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
    -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
    -e ADMIN_ID="$ADMIN_ID" \
    "$IMAGE_NAME" \
    /bin/bash -c "python3 bot.py"

  if [ -n "$current_bots" ]; then
    for bot in $current_bots; do
      echo "Останавливаем старый Telegram бот $bot"
      docker stop "$bot"
      docker rm "$bot"
    done
  fi

  echo "Деплой нового Telegram бота завершен успешно"
}

deploy_another_services() {
  docker stop "worker" || true
  docker rm "worker" || true
  docker stop "worker_heavy" || true
  docker rm "worker_heavy" || true

  for i in 1 2; do
    docker stop "worker_light_${i}" || true
    docker rm "worker_light_${i}" || true
  done

  run_worker() {
    local container_name="$1"
    local worker_pool="$2"

    docker run -d \
      --name "$container_name" \
      --restart always \
      --network infrastructure \
      -v "/certs:/certs" \
      -e WORKER_POOL="$worker_pool" \
      -e PKPASS_PASSWORD="$PKPASS_PASSWORD" \
      -e APPLE_PASS_TYPE_ID="$APPLE_PASS_TYPE_ID" \
      -e APPLE_TEAM_ID="$APPLE_TEAM_ID" \
      -e APPLE_CERTIFICATE_PATH="$APPLE_CERTIFICATE_PATH" \
      -e APPLE_KEY_PATH="$APPLE_KEY_PATH" \
      -e APPLE_WWDR_PATH="$APPLE_WWDR_PATH" \
      -e APPLE_NOTIFICATION_PATH="$APPLE_NOTIFICATION_PATH" \
      -e APPLE_NOTIFICATION_KEY="$APPLE_NOTIFICATION_KEY" \
      -e RABBITMQ_HOST="$RABBITMQ_HOST" \
      -e RABBITMQ_PORT="$RABBITMQ_PORT" \
      -e RABBITMQ_USER="$RABBITMQ_USER" \
      -e RABBITMQ_PASS="$RABBITMQ_PASS" \
      -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
      -e APP_URL="$APP_URL" \
      -e S3_ACCESS="$S3_ACCESS" \
      -e S3_SECRET="$S3_SECRET" \
      -e S3_URL="$S3_URL" \
      -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
      -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
      -e TG_TOKEN="$TG_TOKEN" \
      -e POSTGRES_USER="$POSTGRES_USER" \
      -e POSTGRES_PASS="$POSTGRES_PASS" \
      -e POSTGRES_HOST="$POSTGRES_HOST" \
      -e POSTGRES_PORT="$POSTGRES_PORT" \
      -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
      -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
      -e ADMIN_ID="$ADMIN_ID" \
      "$IMAGE_NAME" \
      /bin/bash -c "python3 worker.py"
  }

  run_worker "worker_heavy" "heavy"

  for i in 1 2; do
    run_worker "worker_light_${i}" "light"
  done

  docker stop "backend_jobs" || true
  docker rm "backend_jobs" || true

  docker run -d \
    --name "backend_jobs" \
    --restart always \
    --network infrastructure \
    -v "/certs:/certs" \
    -e PKPASS_PASSWORD="$PKPASS_PASSWORD" \
    -e APPLE_PASS_TYPE_ID="$APPLE_PASS_TYPE_ID" \
    -e APPLE_TEAM_ID="$APPLE_TEAM_ID" \
    -e APPLE_CERTIFICATE_PATH="$APPLE_CERTIFICATE_PATH" \
    -e APPLE_KEY_PATH="$APPLE_KEY_PATH" \
    -e APPLE_WWDR_PATH="$APPLE_WWDR_PATH" \
    -e APPLE_NOTIFICATION_PATH="$APPLE_NOTIFICATION_PATH" \
    -e APPLE_NOTIFICATION_KEY="$APPLE_NOTIFICATION_KEY" \
    -e RABBITMQ_HOST="$RABBITMQ_HOST" \
    -e RABBITMQ_PORT="$RABBITMQ_PORT" \
    -e RABBITMQ_USER="$RABBITMQ_USER" \
    -e RABBITMQ_PASS="$RABBITMQ_PASS" \
    -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
    -e APP_URL="$APP_URL" \
    -e S3_ACCESS="$S3_ACCESS" \
    -e S3_SECRET="$S3_SECRET" \
    -e S3_URL="$S3_URL" \
    -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
    -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
    -e TG_TOKEN="$TG_TOKEN" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASS="$POSTGRES_PASS" \
    -e POSTGRES_HOST="$POSTGRES_HOST" \
    -e POSTGRES_PORT="$POSTGRES_PORT" \
    -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
    -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
    -e ADMIN_ID="$ADMIN_ID" \
    "$IMAGE_NAME" \
    /bin/bash -c "python3 start_jobs.py"

  docker stop "message_consumer_task" || true
  docker rm "message_consumer_task" || true

  docker run -d \
    --name "message_consumer_task" \
    --restart always \
    --network infrastructure \
    -e PKPASS_PASSWORD="$PKPASS_PASSWORD" \
    -e APPLE_PASS_TYPE_ID="$APPLE_PASS_TYPE_ID" \
    -e APPLE_TEAM_ID="$APPLE_TEAM_ID" \
    -e APPLE_CERTIFICATE_PATH="$APPLE_CERTIFICATE_PATH" \
    -e APPLE_KEY_PATH="$APPLE_KEY_PATH" \
    -e APPLE_WWDR_PATH="$APPLE_WWDR_PATH" \
    -e APPLE_NOTIFICATION_PATH="$APPLE_NOTIFICATION_PATH" \
    -e APPLE_NOTIFICATION_KEY="$APPLE_NOTIFICATION_KEY" \
    -e RABBITMQ_HOST="$RABBITMQ_HOST" \
    -e RABBITMQ_PORT="$RABBITMQ_PORT" \
    -e RABBITMQ_USER="$RABBITMQ_USER" \
    -e RABBITMQ_PASS="$RABBITMQ_PASS" \
    -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
    -e APP_URL="$APP_URL" \
    -e S3_ACCESS="$S3_ACCESS" \
    -e S3_SECRET="$S3_SECRET" \
    -e S3_URL="$S3_URL" \
    -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
    -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
    -e TG_TOKEN="$TG_TOKEN" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASS="$POSTGRES_PASS" \
    -e POSTGRES_HOST="$POSTGRES_HOST" \
    -e POSTGRES_PORT="$POSTGRES_PORT" \
    -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
    -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
    -e ADMIN_ID="$ADMIN_ID" \
    "$IMAGE_NAME" \
    /bin/bash -c "python3 message_consumer.py"

  docker stop "notification_consumer_task" || true
  docker rm "notification_consumer_task" || true

  docker run -d \
    --name "notification_consumer_task" \
    --restart always \
    --network infrastructure \
    -e PKPASS_PASSWORD="$PKPASS_PASSWORD" \
    -e APPLE_PASS_TYPE_ID="$APPLE_PASS_TYPE_ID" \
    -e APPLE_TEAM_ID="$APPLE_TEAM_ID" \
    -e APPLE_CERTIFICATE_PATH="$APPLE_CERTIFICATE_PATH" \
    -e APPLE_KEY_PATH="$APPLE_KEY_PATH" \
    -e APPLE_WWDR_PATH="$APPLE_WWDR_PATH" \
    -e APPLE_NOTIFICATION_PATH="$APPLE_NOTIFICATION_PATH" \
    -e APPLE_NOTIFICATION_KEY="$APPLE_NOTIFICATION_KEY" \
    -e RABBITMQ_HOST="$RABBITMQ_HOST" \
    -e RABBITMQ_PORT="$RABBITMQ_PORT" \
    -e RABBITMQ_USER="$RABBITMQ_USER" \
    -e RABBITMQ_PASS="$RABBITMQ_PASS" \
    -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
    -e APP_URL="$APP_URL" \
    -e S3_ACCESS="$S3_ACCESS" \
    -e S3_SECRET="$S3_SECRET" \
    -e S3_URL="$S3_URL" \
    -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
    -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
    -e TG_TOKEN="$TG_TOKEN" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASS="$POSTGRES_PASS" \
    -e POSTGRES_HOST="$POSTGRES_HOST" \
    -e POSTGRES_PORT="$POSTGRES_PORT" \
    -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
    -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
    -e ADMIN_ID="$ADMIN_ID" \
    "$IMAGE_NAME" \
    /bin/bash -c "python3 notification_consumer.py"
}

apply_migrations() {
  echo "Проверка состояния миграций Alembic..."

  TEMP_CONTAINER="alembic_temp_$(date +%s)"
  docker run -d \
    --name "$TEMP_CONTAINER" \
    --network infrastructure \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASS="$POSTGRES_PASS" \
    -e POSTGRES_HOST="$POSTGRES_HOST" \
    -e POSTGRES_PORT="$POSTGRES_PORT" \
    --entrypoint sleep \
    "$IMAGE_NAME" \
    300

  until docker exec "$TEMP_CONTAINER" ls /backend/alembic.ini &>/dev/null; do
    echo "Ожидание запуска временного контейнера..."
    sleep 2
  done

  run_alembic() {
    docker exec "$TEMP_CONTAINER" python -m alembic "$@"
  }

  echo "Текущий статус миграций:"
  if ! run_alembic current; then
    echo "Не удалось определить текущий статус миграций"
    docker rm -f "$TEMP_CONTAINER" >/dev/null
    return 1
  fi

  # Важно: не создаём merge-ревизии во время деплоя.
  # Это может привести к рассинхронизации истории миграций между окружениями.
  HEAD_REVISIONS=$(run_alembic heads --verbose 2>/dev/null | grep '^Rev:' | awk '{print $2}' || true)
  NUM_HEADS=$(echo "$HEAD_REVISIONS" | sed '/^\s*$/d' | wc -l | tr -d ' ')

  if [ "${NUM_HEADS:-0}" -gt 1 ]; then
    echo "Обнаружено несколько head ($NUM_HEADS). Деплой остановлен: необходимо объединить головы в репозитории."
    echo "$HEAD_REVISIONS" | sed 's/^/ - /'
    docker rm -f "$TEMP_CONTAINER" >/dev/null
    return 1
  fi

  echo "Применяем миграции (alembic upgrade head)..."
  if run_alembic upgrade head; then
    echo "Миграции применены. Текущая ревизия:"
    run_alembic current || true
  else
    echo "Ошибка при применении миграций"
    docker rm -f "$TEMP_CONTAINER" >/dev/null
    return 1
  fi

  docker rm -f "$TEMP_CONTAINER" >/dev/null
}

apply_migrations || {
  echo "Миграции не прошли — отмена деплоя"
  exit 1
}

deploy_new_version
deploy_new_bot_version
deploy_another_services
