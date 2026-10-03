# Практическая работа №5. Контейнеризация и маршрутизация распределённого приложения

Распределённое приложение **АС сервиса доставки**. Сущности и сценарии
взяты из модуля «BaaS для АС сервиса доставки» BootcampLabs: клиент,
продукт, заказ, позиции заказа, статус заказа, адрес. Приложение состоит
из двух **Spring Boot**-сервисов (Order Service и Delivery Service),
базы данных **PostgreSQL** и reverse proxy **Traefik**. Все компоненты
работают в Docker-контейнерах и запускаются одной командой
`docker compose up`.

```
                         Client
                           │  http://localhost/api/...
                           ▼
                 ┌───────────────────┐
                 │      Traefik      │  :80   — единая точка входа
                 │  (Docker labels)  │  :8080 — dashboard
                 └─────────┬─────────┘
   /api/orders/**          │          /api/deliveries/**
   /api/products/**        │
   /api/customers/**       │
             ┌─────────────┴──────────────┐
             ▼                            ▼   балансировка нагрузки
     ┌───────────────┐   RestClient  ┌───────────────────────┐
     │ Order Service │◄──────────────│ Delivery Service  ×3  │
     │ (1 экземпляр) │               │ (stateless-экземпляры)│
     └───────┬───────┘               └───────────────────────┘
             ▼
     ┌───────────────┐
     │  PostgreSQL   │
     └───────────────┘
```

## 1. Используемые технологии (подробно)

### Java 21 и Maven

Оба сервиса написаны на Java 21. В них используются record-классы для DTO
и конфигурации, а также `switch`-выражения. Сборка идёт через **Maven**
(`pom.xml` в каждом сервисе), но устанавливать Maven или JDK на компьютер
**не нужно**: jar собирается внутри Docker-образа (см. раздел «Docker» ниже).
Для запуска нужен только Docker Desktop.

### Spring Boot 3.5

**Spring Boot** — фреймворк поверх Spring. Он сам настраивает приложение по
зависимостям в `pom.xml` (автоконфигурация) и запускает встроенный веб-сервер
Tomcat. Поэтому каждый сервис — обычный исполняемый jar
(`java -jar app.jar`), без отдельного сервера приложений.

В проекте используются стартеры:

| Стартер                          | Сервис       | Зачем                                                         |
| -------------------------------- | ------------ | ------------------------------------------------------------- |
| `spring-boot-starter-web`        | оба          | REST-контроллеры (Spring Web MVC), Tomcat, Jackson (JSON), RestClient |
| `spring-boot-starter-data-jpa`   | order        | Spring Data JPA + Hibernate — работа с БД                     |
| `spring-boot-starter-validation` | оба          | Bean Validation (`@NotNull`, `@NotBlank`, `@Min`...) для входных данных и настроек |
| `spring-boot-starter-actuator`   | оба          | служебные эндпоинты, в первую очередь `/actuator/health` для healthcheck |
| `postgresql`                     | order        | JDBC-драйвер PostgreSQL                                       |

### Spring Web MVC (REST API)

REST API описано аннотациями `@RestController`, `@GetMapping`,
`@PostMapping`, `@PatchMapping`. Jackson автоматически преобразует
Java-объекты в JSON и обратно. Входящие JSON-тела запросов проверяются
аннотацией `@Valid`. Например, при создании заказа без позиций
(`items: []`) клиент сразу получает `400 Bad Request`, и запрос не доходит
до базы данных.

Ошибки возвращаются в стандартном формате **RFC 7807 Problem Details**
(включён настройкой `spring.mvc.problemdetails.enabled: true`):

```json
{"type":"about:blank","title":"Not Found","status":404,"detail":"Заказ 999 не найден","instance":"/api/deliveries/999"}
```

### Spring Data JPA, Hibernate и PostgreSQL

База данных подключена к **Order Service**. Классы модели размечены
аннотациями JPA (`@Entity`, `@Embeddable`, `@ManyToOne`, `@OneToMany`).
**Hibernate** сам создаёт по ним таблицы при запуске (`ddl-auto: update`).
**Spring Data JPA** создаёт реализацию репозиториев по интерфейсу:
достаточно объявить метод `findByStatusInOrderById(...)`, и SQL-запрос
будет построен по имени метода.

Модель повторяет то, что проектировалось в редакторе DataSpace:

```
Customer (агрегат)                 Product (агрегат)
 ├─ personalData: PersonalData      ├─ name: String
 │    (lastName, firstName)         ├─ description: Text
 └─ address: Address                └─ price: BigDecimal
      (city, street, building, flat)       ▲
                                           │ внешняя ссылка
Order (агрегат)                            │
 ├─ customer ──► Customer              OrderList («Список заказа»)
 ├─ comment: Text                       ├─ order  (агрегирующая ссылка на родителя)
 ├─ orderDateTime: LocalDateTime        ├─ product ──► Product
 ├─ deliveryAddress: Address            └─ quantity: Integer
 ├─ status: OrderStatus
 └─ orderListList: List<OrderList>  (One To Many)

OrderStatus (Enum): DRAFT, FIXED, CANCELED, ASSEMBLY, DELIVERY, COMPLETED
```

Как понятия DataSpace перенесены в JPA:

| DataSpace                         | JPA в проекте                                                                 |
| --------------------------------- | ----------------------------------------------------------------------------- |
| Агрегат (корень)                  | `@Entity`: `Customer`, `Product`, `Order`                                      |
| Embeddable-класс                  | `@Embeddable`: `PersonalData`, `Address`. Их поля хранятся в таблице владельца |
| Связь «один ко многим» внутри агрегата | `Order.orderListList` (`@OneToMany(cascade = ALL, orphanRemoval = true)`)   |
| Агрегирующая обратная ссылка      | `OrderList.order` (`@ManyToOne`, `updatable = false`): задаётся при создании и потом не меняется |
| Внешняя ссылка между агрегатами   | `OrderList.product`, `Order.customer` (`@ManyToOne`)                          |
| Enum                              | `OrderStatus` (`@Enumerated(EnumType.STRING)`)                                 |
| Тип `Text`                        | `@Column(columnDefinition = "text")`                                           |
| Тип `BigDecimal`                  | `@Column(precision = 12, scale = 2)`                                           |

Как и сказано в описании модели BootcampLabs, адрес **копируется** в каждый
заказ (`Address.copy()`): адрес клиента может потом измениться, а в заказе
должен остаться тот, по которому его доставляли.

Переходы статусов заказа проверяются в `OrderStatus.nextStatuses()`:

```
DRAFT → FIXED → ASSEMBLY → DELIVERY → COMPLETED
  └───────┴────────┴──► CANCELED
```

При первом запуске пустая БД заполняется демо-данными (`DemoDataLoader`):
5 продуктов, 3 клиента и 5 заказов в разных статусах.

### RestClient — межсервисное HTTP-взаимодействие

**RestClient** — синхронный HTTP-клиент, появившийся в Spring 6.1.
Это современная замена `RestTemplate` с fluent-API. Через него Delivery Service
обращается к Order Service (`OrderClient.java`):

```java
restClient.get()
        .uri("/api/orders/{id}", orderId)
        .retrieve()
        .onStatus(HttpStatusCode::isError, (request, response) -> translateError(response))
        .body(OrderView.class);
```

- Базовый адрес (`http://order-service:8080`) и таймауты не прописаны в коде.
  Они берутся из переменных окружения через `@ConfigurationProperties`
  (`OrderServiceProperties`).
- Запросы выполняет `java.net.http.HttpClient` (`JdkClientHttpRequestFactory`).
  Он выбран потому, что, в отличие от старого `HttpURLConnection`,
  поддерживает метод `PATCH`, которым меняется статус заказа.
- `onStatus(...)` переводит ошибки Order Service в ответы Delivery Service
  (подробнее в разделе «Обработка ошибок»).

### Spring Boot Actuator — проверки состояния

Actuator добавляет служебный эндпоинт `/actuator/health`. Используется
группа **readiness** (`/actuator/health/readiness`), которая отвечает на вопрос
«готов ли экземпляр принимать запросы». В Order Service в неё входит проверка
соединения с БД (`group.readiness.include: readinessState,db`): пока PostgreSQL
недоступна, сервис считается неготовым. Этот же эндпоинт опрашивают
и Docker (healthcheck контейнера), и Traefik (healthcheck балансировщика).

### Docker и многоэтапный (multi-stage) Dockerfile

**Docker** упаковывает приложение со всеми зависимостями в образ, из которого
запускается изолированный контейнер. У каждого сервиса свой `Dockerfile`,
состоящий из двух этапов:

1. **build** (`maven:3.9-eclipse-temurin-21`) — копирует `pom.xml` и `src`,
   выполняет `mvn package` и получает `app.jar`. Локальный репозиторий
   Maven (`~/.m2`) подключён как кэш BuildKit
   (`RUN --mount=type=cache,target=/root/.m2`), поэтому при повторных сборках
   зависимости не скачиваются заново.
2. **runtime** (`eclipse-temurin:21-jre-alpine`) — в итоговый образ попадает
   только JRE и готовый jar, без Maven и исходников. Образ получается
   маленьким. Приложение запускается от непривилегированного пользователя `app`.

### Docker Compose

**Docker Compose** описывает всю систему в одном файле `compose.yaml`:
какие контейнеры запустить, как их связать и в каком порядке поднимать.
Используемые возможности:

- **`build` + `image`** — образ сервиса собирается из его `Dockerfile`;
- **`environment`** и файл **`.env`** — параметры окружения: адреса,
  пароли, тарифы, число экземпляров;
- **`deploy.replicas`** — Delivery Service запускается в нескольких
  экземплярах (по умолчанию 3);
- **`healthcheck`** — команда, которой Docker проверяет, что контейнер
  работает (`healthy`);
- **`depends_on: condition: service_healthy`** — контейнер стартует только
  после того, как его зависимость стала `healthy`;
- **`networks`** — две сети:
  - `baas-edge` — Traefik ↔ сервисы;
  - `baas-backend` (`internal: true`, без выхода наружу) — сервис ↔ сервис
    и сервис ↔ БД. PostgreSQL подключена только к ней;
- **`volumes`** — том `pgdata`, чтобы данные БД сохранялись при перезапуске;
- **встроенный DNS** — контейнеры обращаются друг к другу по имени сервиса
  (`postgres`, `order-service`). Если у сервиса несколько экземпляров,
  DNS возвращает адреса всех.

### Traefik v3 — маршрутизация и балансировка нагрузки

**Traefik** — reverse proxy и балансировщик нагрузки, рассчитанный на работу
с контейнерами. Он подключается к Docker API через `/var/run/docker.sock`,
отслеживает запуск и остановку контейнеров и **сам перестраивает маршруты**
по их меткам (labels). Отдельный файл конфигурации маршрутов не нужен.

Основные понятия Traefik:

| Понятие        | Что это                                                                  | В проекте                                                     |
| -------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------- |
| **EntryPoint** | порт, на котором Traefik принимает запросы                               | `web` — порт 80                                                |
| **Provider**   | откуда Traefik берёт конфигурацию                                        | `docker` — метки контейнеров                                   |
| **Router**     | правило, по которому запрос попадает в сервис                            | `orders`: `PathPrefix(/api/orders)` и др.; `deliveries`: `PathPrefix(/api/deliveries)` |
| **Service**    | группа серверов (контейнеров), между которыми распределяется нагрузка    | `orders` (1 сервер), `deliveries` (3 сервера)                  |
| **Middleware** | обработчик запроса между роутером и сервисом                             | `deliveries-retry` — повтор запроса на другом экземпляре       |

Метки Delivery Service из `compose.yaml`:

```yaml
labels:
  - traefik.enable=true
  - traefik.http.routers.deliveries.entrypoints=web
  - traefik.http.routers.deliveries.rule=PathPrefix(`/api/deliveries`)
  - traefik.http.routers.deliveries.service=deliveries
  - traefik.http.routers.deliveries.middlewares=deliveries-retry
  - traefik.http.middlewares.deliveries-retry.retry.attempts=3
  - traefik.http.services.deliveries.loadbalancer.server.port=8080
  - traefik.http.services.deliveries.loadbalancer.healthcheck.path=/actuator/health/readiness
  - traefik.http.services.deliveries.loadbalancer.healthcheck.interval=5s
  - traefik.http.services.deliveries.loadbalancer.healthcheck.timeout=2s
```

- **Балансировка.** Все экземпляры `delivery-service` имеют одинаковые метки,
  поэтому Traefik объединяет их в **один** сервис `deliveries` с тремя серверами
  и распределяет запросы между ними по очереди (weighted round-robin).
- **Отказоустойчивость** обеспечивается на трёх уровнях:
  1. остановленный контейнер исчезает из Docker API, и Traefik сразу убирает
     его из балансировки;
  2. контейнеры со статусом `starting` или `unhealthy` в балансировку не попадают;
  3. собственный healthcheck балансировщика каждые 5 секунд опрашивает
     `/actuator/health/readiness` у каждого экземпляра. Если же экземпляр
     упал прямо во время обработки запроса, middleware `retry` повторит
     запрос на другом экземпляре.
- Флаг `exposedbydefault=false`: Traefik публикует только контейнеры
  с меткой `traefik.enable=true`. PostgreSQL наружу не попадает.
- **Dashboard** (`--api.dashboard=true`, порт 8080) показывает роутеры,
  сервисы и состояние каждого сервера.

## 2. Сервисы и их функции

### Order Service

Хранит клиентов, продукты и заказы в PostgreSQL: создаёт заказы
и меняет их статусы.

| Метод | Путь                                    | Описание                                                         |
| ----- | --------------------------------------- | ---------------------------------------------------------------- |
| GET   | `/api/products`, `/api/products/{id}`   | продукты                                                         |
| POST  | `/api/products`                         | создать продукт `{name, description, price}`                     |
| GET   | `/api/customers`, `/api/customers/{id}` | клиенты                                                          |
| POST  | `/api/customers`                        | создать клиента `{personalData:{lastName, firstName}, address:{city, street, building, flat}}` |
| GET   | `/api/orders?status=...`                | заказы; фильтр по одному или нескольким статусам необязателен    |
| GET   | `/api/orders/{id}`                      | заказ с позициями и итоговой суммой                              |
| POST  | `/api/orders`                           | создать заказ `{customerId, comment, deliveryAddress?, items:[{productId, quantity}]}`. Если адрес не передан, берётся адрес клиента |
| PATCH | `/api/orders/{id}/status`               | сменить статус `{status}`; недопустимый переход — `409`          |

### Delivery Service

Своей БД у Delivery Service нет. Он получает заказ из Order Service и формирует
по нему **информацию о доставке**: получателя, адрес одной строкой, количество
товаров, стоимость доставки, итог к оплате и плановое время доставки.
Также он передаёт заказ курьеру и закрывает доставку.

| Метод | Путь                                 | Описание                                                       |
| ----- | ------------------------------------ | -------------------------------------------------------------- |
| GET   | `/api/deliveries/{orderId}`          | информация о доставке заказа                                   |
| GET   | `/api/deliveries`                    | активные доставки (заказы в статусах ASSEMBLY и DELIVERY)       |
| POST  | `/api/deliveries/{orderId}/dispatch` | передать заказ курьеру (ASSEMBLY → DELIVERY)                    |
| POST  | `/api/deliveries/{orderId}/complete` | заказ доставлен (DELIVERY → COMPLETED)                          |
| GET   | `/api/deliveries/instance`           | какой экземпляр обработал запрос (для демонстрации балансировки) |

Статус доставки (`DeliveryStatus`) определяется по статусу заказа:

| OrderStatus  | DeliveryStatus | Смысл                       |
| ------------ | -------------- | --------------------------- |
| DRAFT, FIXED | PENDING        | заказ ещё не передан в сборку |
| ASSEMBLY     | ASSEMBLING     | заказ собирается            |
| DELIVERY     | IN_TRANSIT     | курьер в пути               |
| COMPLETED    | DELIVERED      | заказ вручён                |
| CANCELED     | CANCELED       | заказ отменён               |

Стоимость доставки равна `DELIVERY_BASE_COST` (199 ₽). Если сумма заказа
не меньше `DELIVERY_FREE_THRESHOLD` (1500 ₽), доставка бесплатная. Плановое
время — момент оформления заказа плюс `DELIVERY_ESTIMATED_MINUTES` (60 минут).

Каждый ответ Delivery Service обёрнут в `{ "servedBy": {...}, "data": {...} }`.
Блок `servedBy` показывает, **какой экземпляр** обработал запрос. Поле
`hostname` в нём равно короткому `CONTAINER ID` из `docker ps`.

### Сценарий межсервисного взаимодействия

```
Client ── GET /api/deliveries/2 ──► Traefik ──► delivery-service (один из 3 экземпляров)
                                                   │ RestClient
                                                   ▼ GET http://order-service:8080/api/orders/2
                                                 order-service ──► PostgreSQL
                                                   │ заказ: клиент, адрес, позиции, сумма, статус
                     ◄── информация о доставке ────┘
```

Пример ответа:

```json
{
  "servedBy": {
    "hostname": "5069613dad42",
    "ip": "172.19.0.5",
    "instanceId": "f194146f",
    "startedAt": "2026-10-03T12:31:29.004+03:00"
  },
  "data": {
    "orderId": 2,
    "deliveryStatus": "ASSEMBLING",
    "orderStatus": "ASSEMBLY",
    "recipient": "Петрова Анна",
    "address": { "city": "Москва", "street": "Ленинский проспект", "building": "30к2", "flat": "117" },
    "addressLine": "г. Москва, Ленинский проспект, д. 30к2, кв. 117",
    "comment": "Домофон не работает",
    "itemsCount": 2,
    "orderTotal": 1819.00,
    "deliveryCost": 0,
    "totalToPay": 1819.00,
    "orderedAt": "2026-10-03T12:08:03",
    "estimatedDeliveryAt": "2026-10-03T13:08:03"
  }
}
```

Сумма заказа 1819 ₽ больше порога 1500 ₽, поэтому `deliveryCost = 0`.

Запросы на изменение работают так же. `POST /api/deliveries/2/dispatch` →
Delivery Service отправляет `PATCH /api/orders/2/status {"status":"DELIVERY"}`
в Order Service → статус в БД меняется → клиент получает обновлённую
информацию о доставке со статусом `IN_TRANSIT`.

## 3. Структура проекта

```
Практическая работа №5/
├── compose.yaml                         весь стек: traefik, postgres, order-service, delivery-service ×3
├── .env                                 параметры окружения (порты, БД, число экземпляров, тарифы, часовой пояс)
├── demo.ps1                             демонстрация балансировки и остановки экземпляра
├── requests.http                        готовые запросы (VS Code REST Client / IntelliJ HTTP Client)
├── README.md
│
├── order-service/
│   ├── Dockerfile                       многоэтапная сборка: maven → jre-alpine
│   ├── pom.xml                          web, data-jpa, validation, actuator, postgresql
│   └── src/main/
│       ├── resources/application.yml    настройки: подключение к БД из ${DB_URL} и др., actuator
│       └── java/baas/orders/
│           ├── OrderServiceApplication.java     точка входа
│           ├── domain/
│           │   ├── Customer.java                агрегат «Клиент»
│           │   ├── PersonalData.java            embeddable: фамилия, имя
│           │   ├── Address.java                 embeddable: город, улица, здание, квартира
│           │   ├── Product.java                 агрегат «Продукт»
│           │   ├── Order.java                   агрегат «Заказ» (корень) + смена статуса, подсчёт суммы
│           │   ├── OrderList.java               позиция заказа: продукт + количество
│           │   └── OrderStatus.java             enum статусов + допустимые переходы
│           ├── repository/                      Spring Data JPA: Customer-, Product-, OrderRepository
│           ├── service/OrderService.java        создание заказа, смена статуса (транзакции)
│           ├── web/
│           │   ├── OrderController.java         /api/orders
│           │   ├── CatalogController.java       /api/customers, /api/products
│           │   └── Dto.java                     DTO запросов и ответов (records) + валидация
│           └── config/DemoDataLoader.java       демо-данные при пустой БД
│
└── delivery-service/
    ├── Dockerfile                       многоэтапная сборка: maven → jre-alpine
    ├── pom.xml                          web, validation, actuator (без БД)
    └── src/main/
        ├── resources/application.yml    адрес Order Service из ${ORDER_SERVICE_URL}, тарифы доставки
        └── java/baas/delivery/
            ├── DeliveryServiceApplication.java  точка входа
            ├── client/
            │   ├── OrderClient.java             RestClient-клиент Order Service + перевод ошибок
            │   └── OrderView.java               заказ в формате ответа Order Service
            ├── config/
            │   ├── RestClientConfig.java        бин RestClient: baseUrl, таймауты, HttpClient
            │   ├── OrderServiceProperties.java  order-service.* (url, таймауты)
            │   ├── DeliveryProperties.java      delivery.* (стоимость, порог, время доставки)
            │   └── InstanceInfo.java            hostname / ip / id экземпляра для servedBy
            ├── domain/                          DeliveryStatus, OrderStatus, Address
            ├── service/
            │   ├── DeliveryService.java         расчёт информации о доставке
            │   └── DeliveryInfo.java            ответ: информация о доставке
            └── web/
                ├── DeliveryController.java      /api/deliveries
                └── ErrorHandler.java            Order Service недоступен → 503
```

## 4. Конфигурация через переменные окружения

По заданию параметры подключения и адреса сервисов не должны быть прописаны
в исходном коде. В `application.yml` стоят только плейсхолдеры вида
`${DB_URL}`. Их значения задаются в `compose.yaml`, а то, что удобно менять,
вынесено в файл `.env`. Docker Compose читает `.env` автоматически.

| Переменная                                          | Где используется | Назначение                                        |
| --------------------------------------------------- | ---------------- | ------------------------------------------------- |
| `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`              | order-service    | подключение к PostgreSQL                          |
| `SEED_DEMO_DATA`                                    | order-service    | заполнять ли пустую БД демо-данными               |
| `ORDER_SERVICE_URL`                                 | delivery-service | адрес Order Service (`http://order-service:8080`) |
| `ORDER_SERVICE_CONNECT_TIMEOUT`, `ORDER_SERVICE_READ_TIMEOUT` | delivery-service | таймауты RestClient (по умолчанию 2s / 5s) |
| `DELIVERY_BASE_COST`, `DELIVERY_FREE_THRESHOLD`     | delivery-service | стоимость доставки и порог бесплатной доставки    |
| `DELIVERY_ESTIMATED_MINUTES`                        | delivery-service | плановое время доставки                           |
| `DELIVERY_REPLICAS`                                 | compose          | число экземпляров Delivery Service                |
| `HTTP_PORT`, `TRAEFIK_DASHBOARD_PORT`               | compose          | внешние порты Traefik (80 и 8080)                 |
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | compose          | параметры БД                                      |
| `TZ`                                                | compose          | часовой пояс контейнеров (`Europe/Moscow`)        |

`order-service` находит БД по имени `postgres`, а `delivery-service`
находит Order Service по имени `order-service`. Оба имени разрешает встроенный
DNS Docker, поэтому IP-адреса нигде не указываются.

## 5. Healthcheck и порядок запуска

| Контейнер        | Команда проверки                                          | Что проверяет                       |
| ---------------- | --------------------------------------------------------- | ----------------------------------- |
| postgres         | `pg_isready -U orders -d orders`                          | БД принимает подключения            |
| order-service    | `wget -qO- http://localhost:8080/actuator/health/readiness` | приложение запущено и есть связь с БД |
| delivery-service | `wget -qO- http://localhost:8080/actuator/health/readiness` | приложение запущено                 |
| traefik          | `traefik healthcheck --ping`                              | прокси работает                     |

Порядок запуска задаётся через `depends_on` с условием `service_healthy`:

```
postgres ──(healthy)──► order-service ──(healthy)──► delivery-service ×3
traefik стартует сразу и подхватывает сервисы по мере их готовности
```

Так Order Service не пытается подключиться к ещё не готовой базе, а Delivery
Service не начинает принимать запросы, пока Order Service не готов. В выводе
`docker compose up` это видно по строкам `Waiting` → `Healthy` → `Starting`.

## 6. Обработка ошибок

- **Заказ не найден.** Order Service отвечает `404`. `OrderClient` перехватывает
  ответ через `onStatus(...)`, достаёт из Problem Details поле `detail` и бросает
  `ResponseStatusException` с тем же кодом. Клиент Delivery Service получает
  `404 "Заказ 999 не найден"`, а не обезличенную ошибку 500.
- **Недопустимая смена статуса** (например, передать курьеру заказ в статусе
  `DRAFT`). Order Service отвечает `409 Conflict`, и Delivery Service передаёт
  его клиенту с тем же текстом:
  `"Недопустимый переход статуса заказа 1: DRAFT -> DELIVERY (допустимо: [FIXED, CANCELED])"`.
- **Ошибка 5xx в Order Service** — Delivery Service отвечает `502 Bad Gateway`.
- **Order Service недоступен** (контейнер остановлен, истёк таймаут).
  RestClient бросает `ResourceAccessException`, `ErrorHandler` превращает её
  в `503 Service Unavailable` с понятным сообщением.
- **Некорректный запрос** (нет обязательных полей, количество меньше 1).
  Bean Validation возвращает `400 Bad Request`.
- **Экземпляр Delivery Service упал во время обработки запроса.**
  Middleware `retry` в Traefik повторяет запрос на другом экземпляре,
  поэтому клиент ошибки не видит.

## 7. Как запустить приложение и снять скриншоты

Понадобится только **Docker Desktop**, и он должен быть запущен
(`docker info` выполняется без ошибки). Интернет нужен при первой сборке:
Docker скачает образы и Maven-зависимости. Порты **80** и **8080** должны быть
свободны. Если они заняты, поменяйте `HTTP_PORT` и `TRAEFIK_DASHBOARD_PORT` в `.env`.

### Шаг 1. Запустить систему

Откройте PowerShell и перейдите в папку проекта:

```powershell
cd "C:\Users\relax\Documents\PiRKSP_2\Практическая работа №5"
docker compose up --build
```

Первая сборка занимает 1–3 минуты. В логе видно, как контейнеры запускаются
по очереди с учётом healthcheck:

```
 Container baas-delivery-postgres-1          Healthy
 Container baas-delivery-order-service-1     Started
 Container baas-delivery-order-service-1     Healthy
 Container baas-delivery-delivery-service-1  Started
 Container baas-delivery-delivery-service-2  Started
 Container baas-delivery-delivery-service-3  Started
```

Это окно лучше оставить открытым: в нём идут логи всех сервисов. Для
остальных команд откройте **второе окно** PowerShell в той же папке.
Чтобы запустить систему в фоне, используйте `docker compose up --build -d`.

### Шаг 2. Проверить контейнеры

```powershell
docker compose ps
```

```
NAME                               STATUS
baas-delivery-delivery-service-1   Up 27 minutes (healthy)
baas-delivery-delivery-service-2   Up 27 minutes (healthy)
baas-delivery-delivery-service-3   Up 27 minutes (healthy)
baas-delivery-order-service-1      Up 25 minutes (healthy)
baas-delivery-postgres-1           Up 30 minutes (healthy)
baas-delivery-traefik-1            Up 30 minutes (healthy)
```

Все шесть контейнеров должны быть в статусе `(healthy)`.

### Шаг 3. Выполнить запросы через Traefik

Проще всего открыть в браузере:

- <http://localhost/api/orders> — заказы (Order Service);
- <http://localhost/api/products> — продукты;
- <http://localhost/api/deliveries/2> — информация о доставке
  (Delivery Service → Order Service);
- <http://localhost/api/deliveries> — активные доставки.

Можно также открыть `requests.http` в VS Code (расширение REST Client)
или IntelliJ IDEA и нажимать **Send Request** над каждым запросом. Там есть
запросы на создание заказа, смену статуса, передачу курьеру и ошибочный запрос (404).

Из PowerShell:

```powershell
Invoke-RestMethod http://localhost/api/deliveries/2 | ConvertTo-Json -Depth 5
Invoke-RestMethod -Method Post http://localhost/api/deliveries/2/dispatch | ConvertTo-Json -Depth 5
```

### Шаг 4. Посмотреть маршрутизацию в Traefik

Откройте dashboard: <http://localhost:8080/dashboard/>.

- **HTTP → Routers**: роутеры `orders@docker` (правило
  `PathPrefix(/api/orders) || PathPrefix(/api/customers) || PathPrefix(/api/products)`)
  и `deliveries@docker` (правило `PathPrefix(/api/deliveries)`, middleware
  `deliveries-retry`);
- **HTTP → Services → `deliveries@docker`**: три сервера (IP трёх экземпляров),
  у каждого статус `UP`.

### Шаг 5. Показать балансировку и остановку экземпляра

```powershell
.\demo.ps1 -Failover
# если запуск скриптов запрещён:
powershell -ExecutionPolicy Bypass -File .\demo.ps1 -Failover
```

Скрипт:

1. показывает список экземпляров Delivery Service;
2. отправляет 9 запросов `GET /api/deliveries/instance` через Traefik и
   выводит, какой контейнер ответил на каждый (видно чередование);
3. останавливает экземпляр `baas-delivery-delivery-service-1` (`docker stop`);
4. снова отправляет 9 запросов: все успешны, их обрабатывают два оставшихся
   экземпляра;
5. запускает остановленный экземпляр обратно (`docker start`). После
   прохождения healthcheck Traefik снова включает его в балансировку.

### Шаг 6. Что именно скриншотить

1. **Работающие контейнеры** — вывод `docker compose ps` (шаг 2): все
   контейнеры `healthy`, видны три экземпляра `delivery-service`.
2. **Взаимодействие сервисов** — ответ `http://localhost/api/deliveries/2`
   в браузере или в `requests.http`, а рядом логи:
   ```powershell
   docker compose logs --tail 20 delivery-service order-service
   ```
   В логах видна пара строк одного запроса:
   ```
   delivery-service-3 | ... DeliveryService : Запрос информации о доставке заказа 5 -> Order Service
   order-service-1    | ... OrderService    : Запрос заказа 5
   ```
3. **Маршрутизация через Traefik** — dashboard, страница HTTP Routers
   (шаг 4). Можно добавить страницу сервиса `deliveries@docker` с тремя серверами.
4. **Несколько экземпляров одного сервиса** — вывод `.\demo.ps1 -Failover`
   (шаг 5): распределение 3/3/3, затем остановка экземпляра и продолжение
   работы на двух оставшихся.
5. **Прохождение модуля «BaaS для АС сервиса доставки»** — скриншот из BootcampLabs.

### Альтернатива: проверка вручную, без demo.ps1

```powershell
# несколько запросов подряд: hostname в ответах меняется
1..6 | ForEach-Object { (Invoke-RestMethod http://localhost/api/deliveries/instance).hostname }

# сопоставить hostname с именами контейнеров
docker ps --filter "name=delivery-service" --format "table {{.ID}}\t{{.Names}}\t{{.Status}}"

# остановить один экземпляр: запросы продолжают проходить
docker stop baas-delivery-delivery-service-1
1..6 | ForEach-Object { (Invoke-RestMethod http://localhost/api/deliveries/instance).hostname }

# вернуть экземпляр
docker start baas-delivery-delivery-service-1

# изменить число экземпляров без перезапуска остальных
docker compose up -d --scale delivery-service=5
```

### Остановка

```powershell
docker compose down        # остановить и удалить контейнеры (данные БД сохраняются в томе)
docker compose down -v     # то же + удалить том с данными БД (при следующем запуске будут демо-данные)
```

## 8. Пример вывода demo.ps1

```
Экземпляры Delivery Service:
CONTAINER ID   NAMES                              STATUS
750f58c0602a   baas-delivery-delivery-service-3   Up 22 seconds (healthy)
11dfa15b2399   baas-delivery-delivery-service-2   Up 21 seconds (healthy)
8e02e5f51a53   baas-delivery-delivery-service-1   Up 21 seconds (healthy)

=== Балансировка: 9 запросов GET /api/deliveries/instance через Traefik ===
 1. 200  hostname=750f58c0602a  instanceId=d54bbbc2  container=baas-delivery-delivery-service-3
 2. 200  hostname=8e02e5f51a53  instanceId=1fe654b2  container=baas-delivery-delivery-service-1
 3. 200  hostname=11dfa15b2399  instanceId=6e3ab548  container=baas-delivery-delivery-service-2
 4. 200  hostname=8e02e5f51a53  instanceId=1fe654b2  container=baas-delivery-delivery-service-1
 ...
Распределение запросов:
  baas-delivery-delivery-service-1         3
  baas-delivery-delivery-service-2         3
  baas-delivery-delivery-service-3         3

>>> docker stop baas-delivery-delivery-service-1
Экземпляры Delivery Service после остановки:
CONTAINER ID   NAMES                              STATUS
750f58c0602a   baas-delivery-delivery-service-3   Up 23 seconds (healthy)
11dfa15b2399   baas-delivery-delivery-service-2   Up 23 seconds (healthy)
8e02e5f51a53   baas-delivery-delivery-service-1   Exited (143) Less than a second ago

=== Один экземпляр остановлен — доступ через Traefik сохраняется ===
 1. 200  hostname=11dfa15b2399  instanceId=6e3ab548  container=baas-delivery-delivery-service-2
 2. 200  hostname=750f58c0602a  instanceId=d54bbbc2  container=baas-delivery-delivery-service-3
 ...
Распределение запросов:
  baas-delivery-delivery-service-2         5
  baas-delivery-delivery-service-3         4

>>> docker start baas-delivery-delivery-service-1
Экземпляр запущен; после прохождения healthcheck Traefik вернёт его в балансировку.
```

При проверке под нагрузкой один экземпляр остановили во время непрерывной
серии из 40 запросов. Все 40 запросов завершились с кодом `200`.
