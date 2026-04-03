# Clickstream Analytics

Небольшой pet-проект, который я собирал как витринный end-to-end кейс для портфолио.

Идея проекта простая: сгенерировать поток пользовательских событий, прогнать его через стриминговый пайплайн, сохранить данные в аналитическое хранилище, переобучить ML-модель и отдать предсказание через API.

## Что происходит

- генератор создает clickstream-события
- Kafka принимает поток
- Spark Structured Streaming читает события из Kafka и пишет их в ClickHouse
- Airflow запускает обучение модели на агрегированных пользовательских фичах
- FastAPI отдает вероятность покупки по `user_id`

## Схема пайплайна

```text
Generator -> Kafka -> Spark -> ClickHouse -> Airflow -> XGBoost model -> FastAPI
```

## Зачем вообще этот проект

Собирал его как приближенный к реальному production-потоку кейс, чтобы в одном репозитории показать не только ML, но и data engineering часть:

- потоковую обработку событий
- Kafka
- Spark Structured Streaming
- аналитическое хранилище
- orchestration через Airflow
- обучение модели
- inference через API

## Структура проекта

```text
clickstream-analytics/
├── api/
│   ├── main.py
│   └── requirements.txt
├── clickhouse/
│   └── init/
│       ├── 01_raw_events.sql
│       └── 02_user_features_view.sql
├── generator/
│   ├── main.py
│   └── requirements.txt
├── orchestration/
│   └── dags/
│       └── ml_train_dag.py
├── models/
│   └── .gitkeep
├── spark_jobs/
│   ├── processor.py
│   └── requirements.txt
├── docker-compose.yml
└── README.md
```

## Как запустить

### 1. Поднимаем инфраструктуру

```bash
  docker compose up -d
```

### 2. Создаём и активируем виртуальное окружение

#### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

#### Linux / WSL

```bash
    python -m venv .venv
    source .venv/bin/activate
```

### 3. Запускаем генератор событий

```bash
    pip install -r generator/requirements.txt
    python generator/main.py
```

### 4. Запускаем Spark job

```bash
    pip install -r spark_jobs/requirements.txt
    spark-submit spark_jobs/processor.py
```

### 5. Обучаем модель

Airflow здесь нужен как оркестратор для retraining job.

После успешного обучения в папке `models/` должен появиться файл модели.

### 6. Поднять(щиты) API

```bash
    pip install -r api/requirements.txt
    python api/main.py
```

### 7. Проверить предсказание

```bash
  curl http://localhost:8000/predict/42
```

## Что можно было бы улучшить:

В будущем:

- вынести feature transformations в dbt
- добавить метрики качества модели
- подключить трекинг экспериментов
- написать smoke / integration тесты
- добавить мониторинг пайплайна
- полностью завернуть Spark, API и Airflow в отдельные контейнеры

## Чего достигли:

Основная цель была собрать в одном месте стриминг, хранилище, orchestration и ML inference, чтобы показать полный пайплайн, а не только обучение модели.
