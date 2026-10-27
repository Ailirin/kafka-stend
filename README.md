# Нагрузочный стенд Kafka

Воспроизводимые прогоны Apache Kafka 4.1.0 (KRaft) на одной машине: кластер из трёх узлов с ролями broker+controller, продюсер и три независимые consumer group на `confluent-kafka` 2.16.0, снятие метрик в Prometheus и Grafana.

Прогон стартует по `POST /runs`, параметры — переменными окружения. Критерий успеха: `delivered == total_messages`, `failed == 0`, `not_flushed == 0`, лаг трёх групп после догонки — 0. Один изменяемый параметр за прогон, результат — строка в таблице экспериментов.

Брокеры ограничены по CPU и памяти, чтобы цифры отражали железо стенда, а не «сколько влезло в ноут без лимитов».

## Состав

| Сервис | Назначение | URL |
| --- | --- | --- |
| kafka1 / kafka2 / kafka3 | Кластер, 3 брокера, роли broker+controller | внутри Docker |
| kafka-init | Создаёт топик `load-test` (6 партиций, RF=3) и завершается | — |
| producer | HTTP API, отправка по команде | http://localhost:8001/docs, liveness `/health` |
| consumer | Три группы в одном процессе (`group-1` … `group-3`) | http://localhost:8002/metrics |
| kafka-ui | Топики, партиции, consumer groups | http://localhost:8080 |
| jmx-exporter-1/2/3 | JMX брокера, sidecar в сети `kafkaN` | scrape `kafkaN:7071` |
| kafka-exporter | Лаг consumer groups | http://localhost:9308/metrics |
| prometheus | Хранение метрик 7 дней | http://localhost:9090 |
| grafana | Графики, datasource Prometheus уже прописан | http://localhost:3000 (пароль: `GRAFANA_ADMIN_PASSWORD`, дефолт `admin`) |

Порты брокеров наружу не проброшены: advertised-адреса — Docker-имена `kafka1:9092`, `kafka2:9094`, `kafka3:9096`. Клиенты работают только внутри сети Compose.

У каждого брокера heap 512m, лимит контейнера 768m и одно логическое ядро (`cpuset` 0 / 1 / 2). Healthcheck брокера раз в 60 с. Prometheus scrape: 5 с для клиентов, 30 с для JMX.

## Запуск

Нужен Docker Desktop.

```powershell
docker compose up -d
docker compose ps
```

Ожидаемое состояние:

- `kafka1`–`kafka3`, `producer`, `consumer` — `healthy`
- `kafka-init` — `Exited (0)`
- `kafka-ui`, `jmx-exporter-1`–`3`, `kafka-exporter`, `prometheus`, `grafana` — `Up`

Топик создаётся автоматически. Проверка:

```powershell
docker compose logs kafka-init
```

В логе: `PartitionCount: 6`, `ReplicationFactor: 3`.

Консьюмеры стартуют вместе со стендом. Три группы в `Stable` и все 6 партиций у каждой:

```powershell
docker compose logs consumer
docker compose exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --describe --group group-1
```

Продюсер после старта **ничего не отправляет**. Он ждёт HTTP-команды.

## Прогон

По умолчанию прогон — 100 000 сообщений, `acks=1`, `enable.idempotence=false`.

На время отправки UI можно остановить, чтобы не конкурировать с брокерами за CPU:

```powershell
docker compose stop kafka-ui grafana
Invoke-RestMethod -Method Post http://localhost:8001/runs
Invoke-RestMethod http://localhost:8001/runs/current
docker compose start kafka-ui grafana
```

Повторный `POST /runs`, пока идёт отправка, вернёт `409 Conflict`. Параметры прогона (число сообщений, `linger.ms`, `acks` и т. д.) задаются переменными окружения, не телом запроса. В контейнер попадают только те, что прописаны в `docker-compose.yml`.

Ориентир пропускной способности — порядка 100 000 msg/sec. Факт на этом железе ниже; его фиксируют в таблице, а не подгоняют конфигом.

После прогона процесс не завершается: `/metrics` и `GET /runs/current` остаются доступны.

## Как оценить прогон

| Что смотреть | Где |
| --- | --- |
| Брокеры `healthy`, топик 6 партиций / RF=3 | `docker compose ps`, лог `kafka-init` |
| Все сообщения доставлены | `GET /runs/current`: `delivered == total_messages`, `failed == 0`, `not_flushed == 0`. Либо `producer_messages_sent_total` в Prometheus |
| Скорость отправки | поле `rate` в `GET /runs/current` (для короткого прогона надёжнее графика JMX) |
| Группы `Stable`, лаг не растёт после отправки | Kafka UI → Consumers; `kafka_consumergroup_lag` в Grafana |
| Нагрузка на брокеры | Grafana, дашборд ниже |

`kafka_consumergroup_lag` появляется только после первого коммита офсетов. На пустом топике у группы `CURRENT-OFFSET = -`, экспортер лаг не отдаёт.

Смена `linger.ms` / `acks` / сжатия — только пересоздание продюсера, кластер не трогать:

```powershell
docker compose up -d --force-recreate producer
```

Чистый повторный прогон (иначе сообщения копятся в том же топике). Тома брокеров переживают обычный `down`, поэтому сбрасывают только данные Kafka:

```powershell
docker compose down
docker volume rm kafka-stend_kafka1-data kafka-stend_kafka2-data kafka-stend_kafka3-data
docker compose up -d
```

История Prometheus и дашборды Grafana при этом остаются. Полный сброс всех томов: `docker compose down -v`.

## Grafana

Источник Prometheus уже прописан при старте контейнера. Дашборд **Kafka Dashboard** сохранён в томе Grafana.

### 1. Открыть дашборд

Адрес: http://localhost:3000/d/i8HLvrkiz/kafka-dashboard

Анонимный просмотр включён. Чтобы править дашборды, войдите: **Sign in** → `admin` / `admin`. Пароль можно не менять (Skip).

Сверху выставьте фильтры:

- **kafka_job_name** — `kafka-jmx`
- **Broker** — All
- **Topic** — `load-test` или All

Диапазон **Last 15 minutes**, обновление **5s**.

Если страница не открывается: `docker compose ps grafana` должен быть `Up`.

### 2. Проверить источник данных

1. Меню слева (три полоски) → **Connections** → **Data sources**.
2. Должен быть источник **Prometheus**, URL `http://prometheus:9090`.
3. Откройте его → внизу **Save & test**. Должно быть зелёное «Successfully queried the Prometheus API».

Если теста нет — сначала откройте http://localhost:9090/-/ready (должно быть `Prometheus Server is Ready.`), затем перезапустите: `docker compose restart grafana`.

### 3. Проверить, что метрики доходят

http://localhost:9090/targets — все job в статусе **UP**: `producer`, `consumer`, `kafka-exporter`, `kafka-jmx`.

В Grafana: меню → **Drilldown** → **Metrics** (или Explore). Datasource: Prometheus. Запрос:

```promql
up
```

### 4. Запросы дашборда

Готовые дашборды с grafana.com (ID `7589`, `18228`) ждут другие имена метрик и будут пустыми. На **Kafka Dashboard** уже стоят запросы стенда.

| Панель | PromQL | Заметка |
| --- | --- | --- |
| Messages in | `sum(kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate{topic="load-test"})` | скорость записи на брокерах |
| Bytes in | `sum(kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate{topic="load-test"})` | входящий трафик |
| Bytes out | `sum(kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate{topic="load-test"})` | исходящий трафик |
| Consumer lag | `sum(kafka_consumergroup_lag{topic="load-test"}) by (consumergroup)` | появится после первого прогона |
| Producer rate | `rate(producer_messages_sent_total[1m])` | подтверждённые отправки |
| Consumer rate | `sum(rate(consumer_messages_consumed_total[1m])) by (group_id)` | чтение по группам |
| p99 latency | `histogram_quantile(0.99, rate(producer_send_latency_seconds_bucket[1m]))` | задержка produce → callback |
| Broker CPU | `jmx_operatingsystem_processcpuload{job="kafka-jmx"}` | загрузка JVM брокера |
| Broker heap | `jmx_memory_heap_used{job="kafka-jmx"}` | занятая память JVM |
| Broker GC time | `rate(jmx_gc_collectiontime{job="kafka-jmx"}[1m])` | время сборок мусора |
| Produce request p99 | `kafka_network_requestmetrics_totaltimems_99thpercentile{job="kafka-jmx",request="Produce"}` | время запроса Produce на брокере |
| Producer buffer full | `rate(producer_buffer_full_total[1m])` | `produce()` упёрся в локальную очередь |

`OneMinuteRate` из JMX — уже скорость, её нельзя оборачивать в `rate()`. Имена JMX в lowercase: `messagesinpersec`, не `MessagesInPerSec`.

Панели ZooKeeper на дашборде пустые: кластер на KRaft.

### 5. Снять цифры прогона

1. Запустите прогон: `Invoke-RestMethod -Method Post http://localhost:8001/runs`.
2. Смотрите Messages in и Producer rate — должны расти вместе.
3. Lag по группам может вырасти во время отправки и должен перестать расти линейно после неё.
4. Строка таблицы: `rate` и `elapsed_s` из `GET /runs/current`, p99 и пик лага — из Prometheus за окно прогона. Короткий прогон (~3–5 с) на графике JMX легко промахнуть.

## Эксперименты

Один прогон — один изменяемый параметр, остальные фиксированы. Параметры продюсера и топика задаются env **до** `docker compose up`.

```powershell
$env:PRODUCER_LINGER_MS = "100"
docker compose up -d --force-recreate producer
Invoke-RestMethod -Method Post http://localhost:8001/runs
```

Эксперимент с числом партиций (6 → 12) требует пересоздания топика:

```powershell
$env:TOPIC_PARTITIONS = "12"
docker compose down
docker volume rm kafka-stend_kafka1-data kafka-stend_kafka2-data kafka-stend_kafka3-data
docker compose up -d
```

| Переменная | Дефолт | Что меняет |
| --- | --- | --- |
| `TOTAL_MESSAGES` | `100000` | объём прогона |
| `PRODUCER_LINGER_MS` | `50` | накопление батча |
| `PRODUCER_BATCH_SIZE` | `1048576` | макс. размер батча, байт |
| `PRODUCER_COMPRESSION_TYPE` | `snappy` | `snappy` / `lz4` / `none` |
| `PRODUCER_ACKS` | `1` | `1` или `all` |
| `PRODUCER_ENABLE_IDEMPOTENCE` | `false` | идемпотентный продюсер; вместе с ним нужен `acks=all` |
| `PRODUCER_QUEUE_BUFFERING_MAX_MESSAGES` | `200000` | локальная очередь librdkafka |
| `PRODUCER_FLUSH_TIMEOUT_S` | `30` | таймаут финального flush |
| `PRODUCER_MESSAGE_TIMEOUT_MS` | `60000` | сколько сообщение может ждать доставки |
| `TOPIC_PARTITIONS` | `6` | партиции топика (нужен сброс томов брокеров) |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | пароль входа в Grafana |

| № | Изменение | MessagesInPerSec | p99 latency | consumer lag | Замечания |
| --- | --- | --- | --- | --- | --- |
| 1 | Baseline (настройки по умолчанию) | 26 200 | 0,89 с | 0 (пик 57 000) | 3,8 с, delivered 100 000 |
| 2 | `linger.ms` 50 → 100 | 28 800 | 0,94 с | 0 (пик 132 000) | 3,5 с, delivered 100 000 |
| 3 | `linger.ms` 100 → 200 | 34 000 | 0,50 с | 0 (пик 74 000) | 2,9 с, delivered 100 000 |
| 4 | `batch.size` 1 MB → 2 MB | 27 500 | 0,63 с | 0 (пик 145 000) | 3,6 с, delivered 100 000 |
| 5 | `acks` 1 → all | 30 200 | 0,50 с | 0 (пик 70 000) | 3,3 с, delivered 100 000; на одной VM не замедлил |
| 6 | `enable.idempotence` False → True | 20 400 | >1 с | 0 (пик 128 000) | 4,9 с; только вместе с `acks=all`, иначе `_INVALID_ARG` |
| 7 | Партиции 6 → 12 | 20 900 | >1 с | 0 (пик 56 000) | 4,8 с, delivered 100 000 |
| 8 | `compression.type` snappy → lz4 | 26 300 | 0,49 с | 0 (пик 156 000) | 3,8 с, delivered 100 000 |
| 9 | `compression.type` snappy → none | 21 300 | 0,25 с | 0 (пик 177 000) | 4,7 с, delivered 100 000 |

Крупнее батч — выше скорость и длиннее задержка в буфере. `acks=all` и идемпотентность на одной VM не показывают цену репликации по сети; для выводов про географически разнесённый кластер этого стенда недостаточно.

## Границы стенда

- Слушатели PLAINTEXT, без TLS, SASL и ACL: брокеры не опубликованы на хост, клиенты только в сети Compose. Для публичного контура этого недостаточно.
- Полезная нагрузка — короткие текстовые сообщения, один процесс продюсера.
- Логи брокеров, Prometheus и Grafana — в именованных томах. `docker compose down` контейнеры останавливает, данные оставляет.
- Стенд для измерений и регрессии настроек продюсера на изолированной сети Docker.

## Остановка

```powershell
docker compose down
```

Тома (Kafka, Prometheus, Grafana) сохраняются. Чтобы удалить и их:

```powershell
docker compose down -v
```
