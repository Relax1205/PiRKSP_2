# Практическая работа №7. Реактивный HTTP API на Spring WebFlux

Реактивный HTTP-сервис **АС сервиса доставки**. Предметная область та же,
что в предыдущих работах: заказы и статусы из ПР №5, модель доставки
и статусы из смарт-контракта ПР №6, сценарии «получить один объект» и
«получить поток обновлений» из RSocket-приложения ПР №4.

Приложение состоит из двух сервисов на **Spring WebFlux**:

- **Delivery Service** — реактивный CRUD-API доставок (`Mono`/`Flux`),
  поток изменений статуса по **Server-Sent Events** и внешний вызов Order
  Service через **WebClient**;
- **Order Service** — заказы. Написан на функциональных эндпоинтах
  WebFlux (`RouterFunction`), чтобы показать второй стиль.

```
                                 Client (curl, браузер, demo.ps1)
                                   │                          │
                   HTTP-запрос     │                          │  HTTP / SSE
         GET /api/deliveries/{id}  │                          │  GET /api/deliveries/{id}/stream
                                   ▼                          ▼
                              Mono<Delivery>      Flux<ServerSentEvent<DeliveryEvent>>
                              (один ответ)        CREATED → ACCEPTED → IN_TRANSIT → DELIVERED
                                   │                          │
               ┌───────────────────┴──────────────────────────┴───────────────┐
               │                  Delivery Service :8090                      │
               │  DeliveryController → DeliveryService → DeliveryRepository   │
               │                           │        └──► DeliveryEventBus     │
               │                           │             (Sinks: события SSE) │
               │                      OrderClient                             │
               │                      (WebClient)                             │
               └───────────────────────────┬──────────────────────────────────┘
                                           │ GET /api/orders/{id}  (неблокирующий HTTP)
                                           ▼
                          ┌─────────────────────────────────┐
                          │       Order Service :8091       │
                          │  RouterFunction → OrderHandler  │
                          └─────────────────────────────────┘
```

## 1. Используемые технологии (подробно)

### Java 21 и Maven

Оба сервиса написаны на Java 21: record-классы для данных, `switch`-выражения,
pattern matching в `instanceof`. Сборка идёт через **Maven**, но, как и в ПР №5,
устанавливать Maven не нужно: jar собирается внутри Docker-образа.

### Spring Boot 3.5 и Spring WebFlux

**Spring WebFlux** — реактивный веб-фреймворк Spring. Подключается стартером
`spring-boot-starter-webflux` вместо `spring-boot-starter-web` из ПР №5.

| | Spring MVC (ПР №5) | Spring WebFlux (ПР №7) |
|---|---|---|
| Сервер | Tomcat, Servlet API | **Reactor Netty**, без Servlet API |
| Модель потоков | поток на каждый запрос: пока ждём БД или другой сервис, поток простаивает | несколько потоков event loop; ожидание не занимает поток |
| Что возвращает контроллер | готовый объект: `DeliveryInfo`, `List<...>` | «обещание» результата: `Mono<Delivery>`, `Flux<Delivery>` |
| HTTP-клиент | `RestClient` (блокирующий) | `WebClient` (неблокирующий) |
| Потоковая передача | неудобно | `Flux` + `text/event-stream` из коробки |

Используемые стартеры:

| Стартер                          | Сервис   | Зачем                                                    |
| -------------------------------- | -------- | -------------------------------------------------------- |
| `spring-boot-starter-webflux`    | оба      | WebFlux, Reactor Netty, Jackson, WebClient               |
| `spring-boot-starter-validation` | delivery | Bean Validation (`@NotNull`, `@Positive`...) тел запросов |
| `spring-boot-starter-actuator`   | оба      | `/actuator/health` для healthcheck в Docker              |

### Reactor Netty и event loop

WebFlux работает на **Reactor Netty** — неблокирующем сервере на базе Netty
(тот же Netty, что был транспортом RSocket в ПР №4). Запросы обрабатывает
небольшое число потоков **event loop**: по числу ядер процессора, но не меньше 4.
Поток не ждёт ответа Order Service. Он регистрирует «продолжи, когда придёт
ответ» и сразу берётся за другие запросы. Это видно в логах: имена потоков
`reactor-http-epoll-N`, и ответ может обработать не тот поток, который
отправил запрос:

```
[r-http-epoll-11] OrderClient : WebClient -> GET http://order-service:8091/api/orders/1
[r-http-epoll-10] OrderClient : WebClient <- заказ 1 (DRAFT)
```

Поэтому в обработке запросов нельзя вызывать блокирующие операции:
`block()`, `Thread.sleep()`, JDBC. Блокировка event loop останавливает
обработку всех запросов, которые на нём висят. В проекте нет ни одного
вызова `block()`.

### Project Reactor: Mono и Flux

**Project Reactor** — реализация Reactive Streams, на которой построены
WebFlux, WebClient и RSocket (ПР №4).

- **`Mono<T>`** — 0 или 1 элемент: одна доставка, результат создания, `Mono<Void>` для удаления;
- **`Flux<T>`** — 0..N элементов: список доставок, поток событий.

Оба типа **ленивые**: цепочка операторов — это только описание. Ничего не
выполняется, пока кто-то не подпишется (`subscribe`). На `Mono` и `Flux`,
которые возвращает контроллер, подписывается сам WebFlux, когда пишет
ответ клиенту.

### Операторы Project Reactor в проекте

| Оператор                   | Где                                                      | Что делает                                                  |
| -------------------------- | -------------------------------------------------------- | ----------------------------------------------------------- |
| `map`                      | `create`: заказ → новая доставка; контроллер: доставка → `ResponseEntity`, событие → `ServerSentEvent` | синхронно преобразует элемент |
| `filter`                   | `findAll` (фильтр по статусу); `create` (отменённый заказ); `watch` (события только нужной доставки); `simulate` | пропускает только подходящие элементы |
| `flatMap`                  | `details`: доставка → запрос заказа через WebClient; `create`: заказ → сохранение; `update` | преобразует элемент в новый `Mono`/`Flux` (асинхронный шаг) |
| `flatMapMany`              | `watch`: `Mono<Delivery>` → `Flux<DeliveryEvent>`        | из одного элемента делает поток                             |
| `concatMap`                | `courierRoute`: смена статусов строго по очереди         | как `flatMap`, но с сохранением порядка                     |
| `switchIfEmpty`            | `findById`, `delete` (нет доставки → 404); `create` (нет доставки на заказ → запрос в Order Service); Order Service: нет заказа → 404 | что делать, если данных нет |
| `onErrorResume`            | `details` (Order Service недоступен → ответ без заказа); SSE-поток (ошибка → событие `error`); Order Service (неверный id → 400) | заменяет ошибку запасным результатом |
| `onErrorMap`               | `OrderClient`: ошибка соединения → 503                   | превращает одну ошибку в другую                             |
| `retryWhen(Retry.backoff)` | `OrderClient`: повтор запроса при сбое соединения        | повтор с нарастающей паузой                                  |
| `delayElements`, `delayElement` | имитация курьера (пауза между статусами); задержка Order Service | задержка без блокировки потока (таймер)          |
| `takeUntil`                | `watch`: поток заканчивается на финальном статусе        | завершает Flux после элемента, удовлетворяющего условию     |
| `startWith`                | `watch`: сначала текущее состояние, потом изменения      | добавляет элементы в начало потока                          |
| `index`                    | контроллер SSE: номер события → поле `id:`               | нумерует элементы                                           |
| `then`                     | `update`, `delete`                                       | дождаться завершения и перейти к следующему шагу            |
| `doOnNext`, `doOnSubscribe`, `doOnCancel`, `doOnComplete` | логирование, публикация событий | побочные действия, данные не меняются |

### Два стиля WebFlux

1. **Аннотированные контроллеры** (Delivery Service, `DeliveryController`)
   выглядят так же, как в Spring MVC: `@RestController`, `@GetMapping`.
   Разница только в типах результата (`Mono`/`Flux`).
2. **Функциональные эндпоинты** (Order Service, `OrderRoutes` + `OrderHandler`):
   маршруты задаются кодом (`RouterFunction`), а обработчик — функция
   `ServerRequest → Mono<ServerResponse>`:

```java
route()
    .path("/api/orders", builder -> builder
        .GET("", handler::findAll)
        .GET("/{id}", handler::findById))
    .build();
```

### Server-Sent Events (SSE)

**SSE** — стандарт браузеров для потока событий **от сервера к клиенту**
поверх обычного HTTP. Клиент делает один GET-запрос. Сервер отвечает
`Content-Type: text/event-stream`, не закрывает соединение и дописывает
события по мере их появления:

```
id:2
event:ACCEPTED
data:{"deliveryId":4,"type":"ACCEPTED","status":"ACCEPTED","courier":"Смирнов Алексей","at":"2026-10-03T19:16:20"}

```

- `id` — номер события. Если соединение оборвётся, браузер переподключится
  и передаст его в заголовке `Last-Event-ID`;
- `event` — тип события. В браузере на него подписываются
  `source.addEventListener('ACCEPTED', ...)`;
- `data` — данные (JSON). Пустая строка завершает событие.

В WebFlux достаточно вернуть `Flux` с `produces = TEXT_EVENT_STREAM_VALUE`:
каждый элемент `Flux` уходит клиенту отдельным событием сразу, как появился.
В браузере SSE читает встроенный класс `EventSource` (страница `index.html`).

### Sinks — горячий поток событий

Изменения доставок появляются в разных местах: `POST`, `PUT`, `DELETE`,
имитация курьера. Их нужно доставить всем, кто подписан на SSE. Для этого
есть `DeliveryEventBus`:

```java
Sinks.Many<DeliveryEvent> sink = Sinks.many().multicast().directBestEffort();
sink.emitNext(event, ...);   // публикация
sink.asFlux();               // общий Flux для всех подписчиков
```

Это **горячий** поток: события идут независимо от подписчиков, и каждый
подписчик получает их с момента подписки. Поток из репозитория (`findAll`),
наоборот, **холодный**: данные читаются заново для каждого подписчика.

### WebClient — внешний реактивный HTTP-вызов

`WebClient` — неблокирующий HTTP-клиент WebFlux (замена `RestClient` из ПР №5).
Через него Delivery Service получает заказ из Order Service (`OrderClient`):

```java
webClient.get()
        .uri("/api/orders/{id}", orderId)
        .retrieve()
        .onStatus(HttpStatus.NOT_FOUND::equals, r -> Mono.error(Errors.orderNotFound(orderId)))  // 404 -> 422
        .onStatus(HttpStatusCode::is5xxServerError, r -> Mono.error(Errors.orderServiceUnavailable(...)))
        .bodyToMono(OrderView.class)
        .retryWhen(Retry.backoff(props.retries(), Duration.ofMillis(300))
                .filter(OrderClient::isConnectionProblem)                    // только сбои соединения
                .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
        .onErrorMap(OrderClient::isConnectionProblem,
                e -> Errors.orderServiceUnavailable("Order Service недоступен: " + e.getMessage()));  // -> 503
```

- Адрес и таймауты (подключение 2 с, ответ 3 с) задаются в настройках
  (`order-service.*`) и через переменные окружения.
- Повтор делается только при сбое соединения. Повторять 404 бессмысленно:
  заказа от этого не появится.
- В Order Service включена искусственная задержка ответа 300 мс
  (`ORDER_RESPONSE_DELAY`), чтобы было видно, что Delivery Service не
  блокирует поток в ожидании.

### Bean Validation и Problem Details

Тела запросов проверяются аннотациями (`@NotNull`, `@Positive`, `@DecimalMin`)
и `@Valid`. Все ошибки возвращаются в формате **RFC 7807 Problem Details**,
как в ПР №5:

```json
{"type":"about:blank","title":"Not Found","status":404,"detail":"Доставка 999 не найдена","instance":"/api/deliveries/999"}
```

### Docker и Docker Compose

Как в ПР №5: многоэтапный `Dockerfile` (сборка Maven → лёгкий JRE-образ)
и `compose.yaml`. Delivery Service стартует после того, как Order Service
прошёл healthcheck. Порты — **8090** и **8091**, чтобы не пересекаться с
ПР №5 (там на 80 и 8080 работает Traefik).

## 2. API

### Delivery Service (`http://localhost:8090`)

| Метод и путь                          | Тип результата                     | Что делает                                                       | Коды            |
| ------------------------------------- | ---------------------------------- | ---------------------------------------------------------------- | --------------- |
| `GET /api/deliveries`                 | `Flux<Delivery>`                   | все доставки; `?status=IN_TRANSIT` — фильтр                      | 200             |
| `GET /api/deliveries/{id}`            | `Mono<Delivery>`                   | одна доставка                                                    | 200, 404, 400   |
| `POST /api/deliveries`                | `Mono<ResponseEntity<Delivery>>`   | создать доставку по заказу: `{"orderId": 1}`. Данные берутся из Order Service (WebClient) | 201, 400, 409, 422, 503 |
| `PUT /api/deliveries/{id}`            | `Mono<Delivery>`                   | изменить курьера, стоимость, статус: `{"courier": "...", "price": 199, "status": "ACCEPTED"}` | 200, 400, 404, 409 |
| `DELETE /api/deliveries/{id}`         | `Mono<Void>`                       | удалить доставку                                                 | 204, 404        |
| `GET /api/deliveries/{id}/stream`     | `Flux<ServerSentEvent<...>>`, SSE  | поток изменений статуса                                          | 200             |
| `GET /api/deliveries/{id}/details`    | `Mono<DeliveryDetails>`            | доставка + заказ из Order Service                                | 200, 404        |
| `POST /api/deliveries/{id}/simulate`  | `Mono<Delivery>`                   | запустить имитацию курьера (статусы меняются каждые 2 с)         | 202, 404, 409   |
| `GET /`                               | —                                  | демо-страница: таблица, GET (Mono), подписка на SSE              | 200             |

Модель доставки (поля как в смарт-контракте ПР №6):

```json
{
  "id": 1, "orderId": 2,
  "recipient": "Петрова Анна", "address": "г. Москва, Ленинский проспект, д. 30к2, кв. 117",
  "courier": "Кузнецов Дмитрий", "price": 0, "status": "ACCEPTED",
  "createdAt": "2026-10-03T18:54:00", "updatedAt": "2026-10-03T19:04:00"
}
```

Статусы и переходы, как в ПР №6:

```
CREATED ──► ACCEPTED ──► IN_TRANSIT ──► DELIVERED
   │            │
   └────────────┴──► CANCELLED        (после IN_TRANSIT отменить нельзя)
```

Для `ACCEPTED`, `IN_TRANSIT` и `DELIVERED` должен быть указан курьер.
`DELIVERED` и `CANCELLED` — конечные статусы, после них доставку изменить
нельзя. Стоимость считается при создании по тарифу из ПР №5: 199 ₽,
бесплатно от 1500 ₽.

При старте загружаются 3 демо-доставки: для заказов 2 (`ACCEPTED`),
3 (`IN_TRANSIT`) и 4 (`DELIVERED`). Для заказов 1 и 5 доставку можно создать
через `POST`. Заказ 6 отменён, для него `POST` вернёт 409.

### Order Service (`http://localhost:8091`)

| Метод и путь                          | Что делает                                   |
| ------------------------------------- | -------------------------------------------- |
| `GET /api/orders`                     | все заказы; `?status=ASSEMBLY&status=DELIVERY` — фильтр |
| `GET /api/orders/{id}`                | заказ (задержка 300 мс); 404, если нет; 400, если id не число |

Заказы те же, что в демо-данных ПР №5 (клиенты Иванов, Петрова, Сидоров),
плюс отменённый заказ 6.

## 3. Реактивные цепочки

**Один объект и отсутствие данных** — `DeliveryService.findById`:

```java
return repository.findById(id)                                    // Mono<Delivery>, пустой, если нет
        .switchIfEmpty(Mono.error(() -> Errors.deliveryNotFound(id)));  // -> 404
```

**Создание с внешним вызовом** — `DeliveryService.create`:

```java
return repository.findByOrderId(orderId)
        .flatMap(existing -> Mono.<OrderView>error(Errors.conflict("Для заказа ... уже есть доставка")))  // 409
        .switchIfEmpty(Mono.defer(() -> orderClient.getOrder(orderId)))   // доставки нет -> WebClient
        .filter(order -> !order.isCanceled())
        .switchIfEmpty(Mono.error(() -> Errors.conflict("Заказ ... отменён")))  // 409
        .map(this::newDelivery)                                           // заказ -> доставка + стоимость
        .flatMap(repository::save)
        .doOnNext(this::publish);                                         // событие CREATED в SSE
```

**Ответ с запасным вариантом** — `DeliveryService.details`. Если Order
Service недоступен, клиент всё равно получает доставку:

```java
return findById(id)
        .flatMap(delivery -> orderClient.getOrder(delivery.orderId())
                .map(order -> DeliveryDetails.withOrder(delivery, order))
                .onErrorResume(e -> Mono.just(DeliveryDetails.withoutOrder(delivery, reason(e)))));
```

**Поток обновлений** — `DeliveryService.watch` и контроллер:

```java
return findById(id)
        .flatMapMany(current -> eventBus.events()                  // горячий поток всех событий
                .filter(event -> event.deliveryId() == id)          // только эта доставка
                .takeUntil(DeliveryEvent::isLast)                   // до DELIVERED / CANCELLED / удаления
                .startWith(DeliveryEvent.of(current)));             // сначала текущее состояние

// контроллер: Flux<DeliveryEvent> -> Flux<ServerSentEvent>
deliveryService.watch(id)
        .index()
        .map(i -> ServerSentEvent.builder(i.getT2()).id(String.valueOf(i.getT1() + 1)).event(i.getT2().type()).build())
        .onErrorResume(ResponseStatusException.class, e -> Flux.just(/* event: error */));
```

## 4. Обработка ошибок и отсутствия данных

Ошибки обрабатываются внутри реактивной цепочки (`switchIfEmpty`,
`onErrorResume`, `onErrorMap`) и превращаются в Problem Details.

| Ситуация                                       | Где обрабатывается                         | Ответ                                    |
| ---------------------------------------------- | ------------------------------------------ | ---------------------------------------- |
| Доставки нет                                   | `switchIfEmpty(Mono.error(...))`           | 404 `Доставка 999 не найдена`            |
| `id` не число (`/api/deliveries/abc`)          | `ErrorHandler`                             | 400 `Параметр id имеет неверный формат: abc` |
| Тело не прошло валидацию                       | `@Valid` + `ErrorHandler`                  | 400 `Ошибка в теле запроса — orderId: укажите orderId` |
| Нет заказа в Order Service                     | `WebClient.onStatus(404)`                  | 422 `Заказ 999 не найден в Order Service` |
| Заказ отменён / на заказ уже есть доставка     | `filter` + `switchIfEmpty` / `flatMap`     | 409                                      |
| Недопустимый переход (`CREATED → DELIVERED`)   | `checkUpdate` → `Mono.error`               | 409 `Недопустимый переход CREATED -> DELIVERED. Разрешены: [ACCEPTED, CANCELLED]` |
| `ACCEPTED` без курьера                         | `checkUpdate`                              | 400 `Для статуса ACCEPTED нужно указать курьера` |
| Order Service недоступен (`POST`)              | `retryWhen` (2 повтора) → `onErrorMap`     | 503 `Order Service недоступен: ...`      |
| Order Service недоступен (`/details`)          | `onErrorResume`                            | 200, `"order": null`, причина в `orderError` |
| SSE по несуществующей доставке                 | `onErrorResume` в контроллере              | событие `error` с `{"status":404,"detail":...}` |
| Клиент закрыл SSE                              | WebFlux отменяет подписку (`doOnCancel`)   | подписка на шину удаляется, ресурсы освобождаются |

Почему ошибка SSE приходит событием, а не кодом 404: у SSE-ответа статус
`200` и заголовки уже могут быть отправлены, поменять их потом нельзя.
Кроме того, `EventSource` в браузере при ошибочном статусе не показывает
тело, а просто пытается переподключиться. Так же было сделано в канале
RSocket в ПР №4: ошибка по одному заказу приходила сообщением `{"error": ...}`.

## 5. WebFlux и RSocket: сравнение

### Одинаковые сценарии в ПР №4 и ПР №7

| Сценарий               | RSocket (ПР №4)                                              | WebFlux (ПР №7)                                                       |
| ---------------------- | ------------------------------------------------------------ | --------------------------------------------------------------------- |
| Получить один объект   | **Request-Response**: `requestResponse(payload)` → `Mono<Payload>`, маршрут `product.get` в metadata | **HTTP GET + Mono**: `GET /api/deliveries/{id}` → `Mono<Delivery>` |
| Получить поток обновлений | **Request-Stream**: `requestStream(payload)` → `Flux<Payload>`, маршрут `product.stream` / обновления статусов в `order.track` | **HTTP/SSE + Flux**: `GET /api/deliveries/{id}/stream` → `Flux<ServerSentEvent>` |
| Отправить без ответа   | **Fire-and-Forget**: `fireAndForget(payload)` → `Mono<Void>`, ответ не приходит вообще | прямого аналога нет. Ближайшее — `POST /simulate` → **202 Accepted**: работа идёт в фоне, но HTTP-ответ всё равно приходит |
| Двусторонний поток     | **Channel**: `requestChannel(Publisher<Payload>)` ⇄ `Flux<Payload>` | **нет в HTTP API**. SSE односторонний (сервер → клиент); для двустороннего потока нужен WebSocket |

Код сервера:

```java
// ПР №4 — RSocket: один метод на модель, маршрут разбирается вручную из metadata
public Mono<Payload> requestResponse(Payload payload) {
    String route = payload.getMetadataUtf8();            // "product.get"
    ... return Mono.just(toPayload(product));            // JSON вручную через ObjectMapper
}
public Flux<Payload> requestStream(Payload payload) {
    return Flux.fromIterable(store.allProducts()).delayElements(Duration.ofMillis(700)).map(this::toPayload);
}

// ПР №7 — WebFlux: маршрут = URL + HTTP-метод, JSON и SSE формирует фреймворк
@GetMapping("/{id}")
public Mono<Delivery> findById(@PathVariable long id) { ... }

@GetMapping(path = "/{id}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public Flux<ServerSentEvent<Object>> stream(@PathVariable long id) { ... }
```

Код клиента:

```java
// ПР №4 — нужен RSocket-клиент и TCP-соединение
RSocket socket = RSocketConnector.create().connect(TcpClientTransport.create("localhost", 7000)).block();
socket.requestResponse(payload("product.get", "1")).map(ClientMain::readProduct);
socket.requestStream(payload("product.stream", "")).map(ClientMain::readProduct);

// ПР №7 — любой HTTP-клиент: curl, браузер (EventSource), WebClient
//   curl http://localhost:8090/api/deliveries/1
//   curl -N http://localhost:8090/api/deliveries/4/stream
webClient.get().uri("/api/deliveries/{id}", 1).retrieve().bodyToMono(Delivery.class);
webClient.get().uri("/api/deliveries/{id}/stream", 4).accept(MediaType.TEXT_EVENT_STREAM)
        .retrieve().bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<DeliveryEvent>>() {});
```

### Отличия WebFlux от RSocket

Короткий вариант для ответа на задание:

> 1. **Протокол.** WebFlux работает поверх HTTP: каждый запрос — отдельный обмен
>    «запрос → ответ» с методом, URL, заголовками и кодом статуса. RSocket — свой
>    бинарный протокол поверх TCP или WebSocket, все запросы мультиплексируются
>    в одном соединении.
> 2. **Модели взаимодействия.** В RSocket 4 модели встроены в протокол. В HTTP есть
>    только запрос-ответ, а поток (SSE) — это растянутый во времени ответ на один
>    GET. Fire-and-Forget и Channel в HTTP API напрямую не выражаются: ответ на
>    HTTP-запрос приходит всегда, а SSE идёт только от сервера к клиенту.
> 3. **Backpressure.** В RSocket клиент сообщает серверу по сети, сколько элементов
>    готов принять (кадры `REQUEST_N`). В WebFlux backpressure работает внутри
>    приложения, а между клиентом и сервером — только на уровне TCP. SSE-клиент
>    не может попросить «пришли ещё 5».
> 4. **Маршрутизация и ошибки.** В HTTP маршрут — это URL и метод, ошибка — код
>    статуса и Problem Details. В RSocket маршрут передаётся в metadata (в ПР №4 —
>    вручную строкой), ошибка — кадр ERROR, который завершает поток.
> 5. **Клиенты и инфраструктура.** HTTP/SSE понимают браузер (`EventSource`), curl,
>    Postman, прокси и балансировщики (Traefik из ПР №5). Для RSocket нужна
>    специальная клиентская библиотека. Зато RSocket симметричен: сервер тоже
>    может отправлять запросы клиенту, в HTTP запрос инициирует только клиент.

**Что общего.** Оба подхода используют Project Reactor (`Mono`/`Flux`) и Netty
и не блокируют потоки. Поэтому код обработчиков похож. Но `Mono`/`Flux` — это
модель программирования **внутри** приложения. Что и как передаётся по сети,
определяет протокол: HTTP или RSocket. Поэтому WebFlux и RSocket — разные
вещи, хотя типы в коде одинаковые.

## 6. Структура проекта

```
Практическая работа №7/
├── compose.yaml                         запуск обоих сервисов (docker compose up --build)
├── demo.ps1                             демонстрация всех сценариев в консоли
├── requests.http                        запросы для IntelliJ / VS Code REST Client
├── order-service/                       Order Service :8091 — функциональные эндпоинты
│   ├── Dockerfile, pom.xml
│   └── src/main/java/baas/orders/
│       ├── domain/                      Order (record), OrderStatus
│       ├── repository/OrderRepository   заказы в памяти, Mono/Flux
│       └── web/
│           ├── OrderRoutes              RouterFunction: GET /api/orders, /api/orders/{id}
│           └── OrderHandler             обработчики: filter, switchIfEmpty, onErrorResume, delayElement
└── delivery-service/                    Delivery Service :8090 — аннотированные контроллеры
    ├── Dockerfile, pom.xml
    └── src/main/
        ├── java/baas/delivery/
        │   ├── domain/                  Delivery, DeliveryStatus (переходы), DeliveryEvent
        │   ├── repository/              DeliveryRepository — доставки в памяти, Mono/Flux
        │   ├── client/                  OrderClient (WebClient), OrderView
        │   ├── service/
        │   │   ├── DeliveryService      вся логика: CRUD, details, watch (поток), simulate
        │   │   ├── DeliveryEventBus     Sinks.Many — горячий поток событий для SSE
        │   │   └── DeliveryDetails      доставка + заказ
        │   ├── web/
        │   │   ├── DeliveryController   REST + SSE
        │   │   ├── Dto                  тела запросов с валидацией
        │   │   └── ErrorHandler         тексты ошибок (Problem Details)
        │   ├── error/Errors             фабрика ResponseStatusException (404, 409, 422, 503)
        │   └── config/                  WebClient, настройки, демо-данные
        └── resources/
            ├── application.yml
            └── static/index.html        демо-страница: GET (Mono) и подписка на SSE в браузере
```

**Почему данные в памяти, а не в PostgreSQL, как в ПР №5.** JPA и JDBC
блокирующие: поток ждёт ответа БД. В WebFlux их нельзя вызывать на event
loop. Нужен либо реактивный драйвер **R2DBC** (`spring-boot-starter-data-r2dbc`),
либо вынос вызовов на отдельный пул (`subscribeOn(Schedulers.boundedElastic())`).
Работа посвящена HTTP-взаимодействию, поэтому хранилище сделано в памяти,
но с реактивным интерфейсом (`Mono`/`Flux`). Заменить его на R2DBC-репозиторий
можно без изменения сервиса и контроллера.

## 7. Как запустить и снять скриншоты

Понадобится только **Docker Desktop**. JDK и Maven не нужны: сборка идёт
внутри образа.

### Шаг 1. Запустить систему

```powershell
cd "C:\Users\relax\Documents\PiRKSP_2\Практическая работа №7"
docker compose up --build -d
```

Первая сборка занимает 2–4 минуты (Maven скачивает зависимости), повторные —
секунды. Проверить:

```powershell
docker compose ps
```

Оба контейнера должны быть в состоянии `Up ... (healthy)`.

Если порты 8090/8091 заняты, поменяйте их в `compose.yaml`.

### Шаг 2. Прогнать демонстрацию

```powershell
powershell -ExecutionPolicy Bypass -File .\demo.ps1
```

Скрипт по очереди выполняет все сценарии и печатает результаты (раздел 8).
Его можно запускать повторно: доставка из прошлого прогона удаляется.

### Шаг 3. SSE в двух окнах (наглядно для скриншота)

Порядок важен: сначала создать доставку, потом подписаться, потом
запустить курьера. Для несуществующей доставки поток сразу отдаёт
`event:error` и закрывается.

Окно 2 — создать доставку:

```powershell
curl.exe -s -X POST -H "Content-Type: application/json" -d '{\"orderId\": 1}' http://localhost:8090/api/deliveries
```

В ответе будет `"id":4`. Если перед этим запускали `demo.ps1`, номер будет
больше (5, 6...): подставьте его в команды ниже. Если доставка для заказа 1
уже есть (ответ 409), возьмите заказ 5.

Окно 1 — подписка на поток (`-N` отключает буферизацию, `-s` прячет прогресс):

```powershell
curl.exe -sN http://localhost:8090/api/deliveries/4/stream
```

Окно 2 — запустить курьера:

```powershell
curl.exe -s -X POST http://localhost:8090/api/deliveries/4/simulate
```

В окне 1 события появляются **по одному, с паузой 2 секунды**:
`CREATED → ACCEPTED → IN_TRANSIT → DELIVERED`. После `DELIVERED` сервер
закрывает поток, и curl завершается.

### Шаг 4. Демо-страница в браузере

Откройте <http://localhost:8090/>:

- **Обновить** — `GET /api/deliveries` (Flux → таблица);
- **Создать доставку** — `POST` по номеру заказа (WebClient → Order Service);
- **GET (Mono)** — одна доставка, видны код ответа и JSON;
- **Подписаться (SSE)**, затем **Запустить курьера** — события появляются
  в списке с отметкой времени, таблица обновляется.

Сырой поток SSE можно посмотреть в DevTools (F12) → **Network** → запрос
`stream` → вкладка **EventStream**.

### Шаг 5. Что именно скриншотить

1. **Реактивные HTTP-запросы (2–4 скриншота)** — вывод `demo.ps1`:
   - блоки 1 и 3: `GET` списка (Flux) и одной доставки (Mono);
   - блоки 4, 6, 7: обработка ошибок (404, 422, 409);
   - блоки 5 и 8: `POST` и `details` — внешний вызов WebClient к Order Service.

   Как вариант — те же запросы в браузере (`http://localhost:8090/api/deliveries/1`)
   или в `requests.http`.
2. **Потоковая передача через SSE** — блок 9 `demo.ps1`: события с
   разными метками времени. Или окно 1 из шага 3, или демо-страница
   с журналом событий. Хорошо видно, что события приходят постепенно,
   а не одним ответом.
3. **Отличия WebFlux от RSocket** — текстом из раздела 5 (цитата с пятью
   пунктами).
4. **Логи** (необязательно, но наглядно):
   `docker compose logs delivery-service` — видны потоки `reactor-http-epoll-N`,
   вызовы WebClient, подписка и завершение SSE.
5. **BootcampLabs** — страница текущего прогресса.

### Остановка

```powershell
docker compose down
```

Данные хранятся в памяти: после перезапуска снова будут 3 демо-доставки.

## 8. Пример вывода demo.ps1

```
=== 1) GET /api/deliveries — несколько объектов: Flux<Delivery> -> JSON-массив ===
GET http://localhost:8090/api/deliveries -> HTTP 200

id orderId recipient    courier          price status
-- ------- ---------    -------          ----- ------
 1       2 Петрова Анна Кузнецов Дмитрий     0 ACCEPTED
 2       3 Сидоров Пётр Смирнов Алексей    199 IN_TRANSIT
 3       4 Иванов Иван  Кузнецов Дмитрий   199 DELIVERED

=== 3) GET /api/deliveries/1 — один объект: Mono<Delivery> (аналог RSocket Request-Response) ===
GET http://localhost:8090/api/deliveries/1 -> HTTP 200

id        : 1
orderId   : 2
recipient : Петрова Анна
address   : г. Москва, Ленинский проспект, д. 30к2, кв. 117
courier   : Кузнецов Дмитрий
price     : 0
status    : ACCEPTED
...

=== 4) GET /api/deliveries/999 — нет данных: switchIfEmpty -> 404 Problem Details ===
GET http://localhost:8090/api/deliveries/999 -> HTTP 404

title  : Not Found
status : 404
detail : Доставка 999 не найдена

=== 5) POST /api/deliveries — WebClient: Delivery Service -> Order Service -> новая доставка ===
POST http://localhost:8090/api/deliveries -> HTTP 201

id        : 4
orderId   : 1
recipient : Иванов Иван
address   : г. Москва, Тверская, д. 12, кв. 45
price     : 199
status    : CREATED

=== 6) POST для несуществующего и отменённого заказа — ошибки WebClient в реактивной цепочке ===
POST http://localhost:8090/api/deliveries -> HTTP 422
detail : Заказ 999 не найден в Order Service
POST http://localhost:8090/api/deliveries -> HTTP 409
detail : Заказ 6 отменён, доставка невозможна

=== 7) PUT /api/deliveries/4 — запрещённый переход CREATED -> DELIVERED ===
PUT http://localhost:8090/api/deliveries/4 -> HTTP 409
detail : Недопустимый переход CREATED -> DELIVERED. Разрешены: [ACCEPTED, CANCELLED]

=== 9) GET /api/deliveries/4/stream — SSE: Flux событий (аналог RSocket Request-Stream) ===
Через 2 с запускается курьер (POST /simulate). События приходят по мере смены статуса:
[19:16:20] id:1
[19:16:20] event:CREATED
[19:16:20] data:{"deliveryId":4,"type":"CREATED","status":"CREATED","courier":null,"at":"2026-10-03T19:16:13"}
[19:16:24] id:2
[19:16:24] event:ACCEPTED
[19:16:24] data:{"deliveryId":4,"type":"ACCEPTED","status":"ACCEPTED","courier":"Смирнов Алексей","at":"2026-10-03T19:16:20"}
[19:16:26] id:3
[19:16:26] event:IN_TRANSIT
[19:16:26] data:{"deliveryId":4,"type":"IN_TRANSIT","status":"IN_TRANSIT","courier":"Смирнов Алексей","at":"2026-10-03T19:16:22"}
[19:16:28] id:4
[19:16:28] event:DELIVERED
[19:16:28] data:{"deliveryId":4,"type":"DELIVERED","status":"DELIVERED","courier":"Смирнов Алексей","at":"2026-10-03T19:16:24"}
Сервер закрыл поток после финального статуса DELIVERED

=== 10) DELETE /api/deliveries/4 ===
DELETE http://localhost:8090/api/deliveries/4 -> HTTP 204
GET http://localhost:8090/api/deliveries/4 -> HTTP 404
```

Время в начале строки — момент, когда строка пришла клиенту, по часам
Windows. По нему видно, что события приходят с интервалом около 2 секунд.
Поле `at` — время изменения по часам контейнера. Часы виртуальной машины
Docker Desktop могут отставать от Windows на несколько секунд и
подстраиваться рывками, поэтому `at` иногда расходится с локальным
временем. На логику это не влияет: порядок событий задаёт поток, а не
время.

## 9. Ответы на возможные вопросы

**Чем WebFlux отличается от Spring MVC из ПР №5?**
В MVC на каждый запрос выделяется поток, и он простаивает, пока ждёт БД или
другой сервис. В WebFlux несколько потоков event loop обслуживают все
запросы. Ожидание ответа не занимает поток: обработка продолжается, когда
данные пришли. Поэтому WebFlux выдерживает много одновременных медленных
соединений (например, открытых SSE-потоков) на малом числе потоков.

**Почему нельзя вызывать `block()`?**
`block()` останавливает текущий поток до получения результата. В WebFlux
это поток event loop, и вместе с ним встанут все запросы, которые он
обслуживает. Reactor Netty прямо запрещает `block()` на своих потоках
и бросает `IllegalStateException`. В проекте нет ни одного вызова `block()`.

**Кто подписывается на `Mono`, который возвращает контроллер?**
WebFlux. Контроллер возвращает описание цепочки, фреймворк подписывается
на неё и записывает результат в HTTP-ответ, когда он готов. Если клиент
отключится раньше, WebFlux отменит подписку (`cancel`).

**Чем `map` отличается от `flatMap`?**
`map` синхронно превращает элемент в другой объект: заказ → доставка.
`flatMap` превращает элемент в новый `Mono`/`Flux`, то есть в асинхронную
операцию (доставка → HTTP-запрос заказа), и подписывается на неё.
`concatMap` делает то же, но строго по одному элементу, сохраняя порядок —
он нужен при смене статусов курьером.

**Как обрабатывается отсутствие данных?**
Репозиторий возвращает пустой `Mono`, а не `null`. `switchIfEmpty` задаёт
действие на этот случай: ошибку 404 (`findById`) или альтернативный источник
(`create`: доставки на заказ нет → идём в Order Service).

**Чем горячий поток отличается от холодного?**
Холодный (`repository.findAll()`) начинает выдавать данные заново для
каждого подписчика. Горячий (`DeliveryEventBus`, `Sinks`) выдаёт события
независимо от подписчиков. Кто подписался позже, получит только новые
события. Поэтому в `watch` текущее состояние добавляется в начало потока
через `startWith`.

**Что происходит, когда клиент закрывает SSE?**
WebFlux отменяет подписку на `Flux` (в логе `SSE: клиент отключился от
доставки N`). Подписка на шину событий удаляется, утечки нет. Если
соединение оборвалось, браузерный `EventSource` переподключается сам
и передаёт `Last-Event-ID`.

**Почему SSE, а не WebSocket?**
По заданию нужен поток от сервера к клиенту, а это ровно то, что делает SSE:
обычный HTTP GET, работает через прокси и балансировщики, браузер
поддерживает его встроенным `EventSource` с автопереподключением.
WebSocket — двусторонний протокол: он понадобился бы для аналога RSocket
Channel.

**Почему для несуществующего заказа 422, а не 404?**
404 означал бы «не найден запрошенный ресурс», то есть `/api/deliveries`.
Здесь запрос корректен по форме, но ссылается на заказ, которого нет.
Для этого подходит 422 Unprocessable Entity.

**Зачем `retryWhen` и почему повтор только при сбое соединения?**
Если Order Service на мгновение недоступен (перезапуск), повтор через
300 и 600 мс часто помогает. 404 или 409 при повторе не изменятся,
поэтому их не повторяем. `GET` можно безопасно повторять: он не меняет
данные.

**Как `/simulate` связан с Fire-and-Forget?**
Имитация курьера запускается в фоне через `subscribe()`, а клиент сразу
получает `202 Accepted`. По смыслу это похоже на Fire-and-Forget, но
HTTP-ответ всё равно приходит. В RSocket Fire-and-Forget ответа нет
совсем, даже подтверждения.

**`GET /api/deliveries` возвращает `Flux`, почему клиент получает JSON-массив?**
WebFlux выбирает формат по заголовку `Accept`. Для `application/json`
весь `Flux` собирается в массив. Если запросить тот же адрес с
`Accept: text/event-stream`, тот же `Flux` уйдёт как SSE, по событию
на элемент (пример в `requests.http`).

**Можно ли запустить Delivery Service в нескольких экземплярах, как в ПР №5?**
CRUD — да, если вынести данные в общую БД. SSE — с доработкой: шина
событий (`Sinks`) живёт в памяти одного экземпляра. Подписчик на
экземпляре A не узнает об изменении на экземпляре B. Нужен общий брокер
(Redis Pub/Sub, Kafka), из которого каждый экземпляр читает события.
