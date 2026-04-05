# pulse-backend — Техническое задание
> API Gateway, авторизация, multi-tenancy поверх pulse-core  
> Репозиторий: `synthetis/pulse-backend` · Стек: Go

---

## Суть

`pulse-backend` — HTTP/WebSocket сервер, который стоит между клиентами и `pulse-core`.  
Решает три задачи: **авторизация**, **изоляция данных между пользователями**, **лимиты**.

`pulse-core` при этом ничего не знает о пользователях — это просто хранилище.

```
SDK клиент (Go/Python)
    │  API ключ
    ▼
┌─────────────────────────────────┐
│         solenix-backend           │
│  auth · tenancy · limits · ws   │
└────────────────┬────────────────┘
                 │ прямой импорт или gRPC
                 ▼
         solenix-core (DB)
```

---

## Изоляция данных (multi-tenancy)

**Подход: `_tenant` label.**

Бэкенд добавляет `_tenant: <userID>` к каждому запросу перед передачей в ядро.  
Пользователь не может указать этот label сам — он проставляется серверной стороной.

```
Write  →  добавить _tenant → db.WriteBatch(metric, {_tenant: id, ...labels}, points)
Query  →  добавить _tenant → db.Query(metric, {_tenant: id, ...labels}, from, to)
Delete →  добавить _tenant → db.Delete(metric, {_tenant: id, ...labels}, from, to)
Stream →  добавить _tenant → db.Subscribe(metric, {_tenant: id, ...labels})
```

Изоляция гарантируется на уровне pulse-core: фильтрация по labels происходит в ядре,  
данные физически хранятся в одной DB но разделены по series.

---

## Стек

| Компонент | Выбор |
|---|---|
| HTTP роутер | `fiber` |
| БД для пользователей | PostgreSQL (users, api_keys) |
| JWT | `golang-jwt/jwt` |
| WebSocket | `gorilla/websocket` |
| pulse-core | прямой Go импорт (не gRPC) |

---

## Структура проекта

```
pulse-backend/
├── cmd/main.go
├── internal/
│   ├── auth/
│   │   ├── jwt.go          # генерация и валидация JWT
│   │   ├── apikey.go       # генерация и валидация API ключей
│   │   └── middleware.go   # chi middleware для обоих типов авторизации
│   ├── tenant/
│   │   └── inject.go       # добавляет _tenant label в запросы к ядру
│   ├── limits/
│   │   └── limits.go       # подсчёт объёма данных, блокировка при превышении
│   ├── ws/
│   │   └── hub.go          # WebSocket хаб, подписка через db.Subscribe
│   └── handler/
│       ├── write.go
│       ├── query.go
│       ├── delete.go
│       ├── auth.go         # /register, /login, /keys
│       └── health.go 
├── db/
│   └── postgres.go         # подключение к PostgreSQL, миграции
└── config.yaml
```

---

## База данных (PostgreSQL)

```sql
CREATE TABLE users (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email      TEXT UNIQUE NOT NULL,
    password   TEXT NOT NULL,        -- bcrypt hash
    plan       TEXT NOT NULL DEFAULT 'free',  -- 'free' | 'pro'
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE api_keys (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID REFERENCES users(id) ON DELETE CASCADE,
    key_hash   TEXT UNIQUE NOT NULL,  -- SHA-256 хеш ключа, сам ключ не хранится
    name       TEXT,                  -- метка ключа ("prod server", "laptop")
    created_at TIMESTAMPTZ DEFAULT now()
);
```

---

## API

### Авторизация

| Метод | Путь | Auth | Описание |
|---|---|---|---|
| POST | `/v1/auth/register` | — | Регистрация, возвращает JWT |
| POST | `/v1/auth/login` | — | Логин, возвращает JWT |
| POST | `/v1/auth/keys` | JWT | Создать API ключ |
| GET | `/v1/auth/keys` | JWT | Список API ключей пользователя |
| DELETE | `/v1/auth/keys/:id` | JWT | Удалить API ключ |

### Данные

| Метод | Путь | Auth | Описание |
|---|---|---|---|
| POST | `/v1/write` | API key | Записать точки |
| GET | `/v1/query` | JWT или API key | Прочитать данные |
| GET | `/v1/query/agg` | JWT или API key | Агрегированные данные |
| DELETE | `/v1/delete` | API key | Удалить точки |
| GET | `/v1/metrics` | JWT или API key | Список метрик пользователя |
| GET | `/v1/health` | — | Healthcheck |

### WebSocket

| Путь | Auth | Описание |
|---|---|---|
| `WS /v1/stream` | JWT или API key | Real-time подписка на метрику |

---

## Форматы запросов

### POST /v1/write
```json
{
  "series": [
    {
      "metric": "cpu.usage",
      "labels": { "host": "server-1" },
      "points": [
        { "timestamp": 1712000000000000000, "value": 73.5 }
      ]
    }
  ]
}
```

### GET /v1/query
```
?metric=cpu.usage&labels=host:server-1&from=1712000000000000000&to=1712003600000000000
```

Ответ:
```json
{
  "series": [
    {
      "metric": "cpu.usage",
      "labels": { "host": "server-1" },
      "points": [
        { "timestamp": 1712000000000000000, "value": 73.5 }
      ]
    }
  ]
}
```

### GET /v1/query/agg
```
?metric=cpu.usage&labels=host:server-1&from=...&to=...&window=60s&agg=avg
```
`agg`: `avg` | `min` | `max` | `sum` | `count`

### WS /v1/stream
После подключения клиент отправляет подписку:
```json
{ "metric": "cpu.usage", "labels": { "host": "server-1" } }
```
Сервер шлёт точки по мере поступления:
```json
{ "timestamp": 1712000001000000000, "value": 74.1 }
```

---

## Авторизация — детали

### Два типа токенов

**JWT** — для фронтенда. Выдаётся при логине, живёт 24 часа.  
Содержит: `user_id`, `email`, `plan`, `exp`.

**API ключ** — для SDK. Генерируется пользователем, не истекает.  
Формат: `pulse_<32 random hex bytes>`.  
В базе хранится только SHA-256 хеш. При запросе: хешируем входящий ключ → ищем в `api_keys`.

### Middleware

```
Запрос
  │
  ├── Authorization: Bearer <JWT>    →  validateJWT()  →  ctx с user_id
  ├── Authorization: Bearer pulse_.. →  validateAPIKey() →  ctx с user_id
  └── нет заголовка                  →  401
```

---

## Лимиты (free tier)

- **2 GB** — максимальный объём данных для плана `free`
- Подсчёт: при каждом `/v1/write` бэкенд считает примерный размер точек и суммирует по `user_id`
- Хранить счётчик в памяти (Redis в будущем), сбрасывать при рестарте незначительно
- При превышении: `HTTP 429` с `{"error": "storage limit exceeded", "limit_gb": 2}`

Простая формула размера одной точки: `16 байт` (8 timestamp + 8 value).

---

## Что НЕ входит в этот этап

- Email подтверждение при регистрации
- Смена пароля / восстановление
- Биллинг и оплата подписки
- Rate limiting по запросам (только по объёму данных)
- Удаление аккаунта и всех данных

---

## Порядок реализации

1. **Основа** — chi роутер, `/health`, конфиг, подключение к PostgreSQL и pulse-core
2. **Auth** — register/login, JWT middleware, API ключи
3. **Write/Query** — основные эндпоинты с tenant изоляцией
4. **WebSocket** — real-time стриминг
5. **Лимиты** — подсчёт объёма, блокировка при превышении
6. **Delete + Metrics** — вспомогательные эндпоинты
