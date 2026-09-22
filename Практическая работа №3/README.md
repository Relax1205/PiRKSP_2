# Практическая работа №3 — Реактивное программирование с использованием RxJava

## Кратко о работе

Приложение обрабатывает данные АС Поставщиков (поставщик/товар/цена/количество) не обычным циклом,
а как реактивный поток событий: данные поступают из источника, автоматически проходят цепочку
`фильтрация → преобразование → группировка/агрегация → результат`, при этом некорректные записи не роняют
программу, а часть шагов считается в фоновых потоках. Дополнительно показана имитация «живых» данных,
приходящих во времени, и ситуация, когда источник производит элементы быстрее, чем их успевает обработать
подписчик (backpressure).

Модель данных (`SupplyRecord`: supplierId, supplierName, product, price, quantity) повторяет по смыслу
сущности `Darkstore` / `Good` / `GoodsList` модуля «Back-end АС Поставщиков»
(лежит рядом, в `bootcamp-darkstore-provider-relax1069-4h2fd`): поставщик = дарксторовский склад
(`Darkstore.id/address`), товар = `Good.name/price`, количество = `GoodsList.count`. К базе данных или
backend-коду этого модуля приложение не обращается — только повторяет структуру данных и реализует для неё
реактивную обработку на RxJava.

## Технологии

- **Java 21** (JDK, `javac`/`java`) — язык и рантайм. Специфичных для новых версий Java возможностей не
  используется, минимально совместимая версия — Java 8+.
- **RxJava 3** (`io.reactivex.rxjava3`, artifact `io.reactivex.rxjava3:rxjava:3.1.8`) — основная библиотека
  реактивного программирования. Из неё используются:
  - **`Observable<T>`** — реактивный поток без встроенного управления скоростью потребления (push-модель);
    основной тип для демонстрации базовой цепочки и имитации live-данных.
  - **`Flowable<T>`** — аналог `Observable`, но поддерживает backpressure (реализует спецификацию
    Reactive Streams через интерфейсы `Publisher`/`Subscriber`); используется для демонстрации ситуации,
    когда источник быстрее обработчика.
  - **`PublishSubject<T>`** — одновременно и `Observable`, и «ручной» источник/приёмник событий: в него
    вручную кладут (`onNext`) новые элементы из отдельного потока, имитируя поступление данных во времени.
  - **Операторы**: `fromIterable`, `filter`, `map`, `flatMap`, `flatMapSingle`, `groupBy`, `reduce`, `toList`,
    `buffer(time, TimeUnit)`, `onErrorResumeNext`, `doOnNext`, `onBackpressureDrop`.
  - **`Schedulers`** — пул потоков для планирования работы: `Schedulers.io()` (для «источника», имитирует
    блокирующий ввод-вывод), `Schedulers.computation()` (для CPU-вычислений/агрегации). Переключение между
    ними выполняется операторами `subscribeOn`/`observeOn`.
  - **`BackpressureStrategy`** (`ERROR`, `MISSING`) и `MissingBackpressureException` — механизм и исключение
    Reactive Streams, которые показывают, что происходит, когда потребитель не успевает запрашивать элементы
    быстрее, чем их поставляет источник.
- **Reactive Streams** (`org.reactivestreams:reactive-streams:1.0.4`) — стандартный API (`Publisher`,
  `Subscriber`, `Subscription`), на котором построен `Flowable`; подключается как транзитивная зависимость
  RxJava.
- Сборка и запуск — «голым» `javac`/`java` без Maven/Gradle: зависимости лежат в виде готовых `.jar`
  в `lib/` и подключаются через classpath в `run.bat` (тот же подход, что и в практической работе №2).

## Как запустить

Требуется установленный JDK 8+ (проверено на JDK 21), Maven не нужен — все зависимости уже скачаны в `lib/`.

```
run.bat
```

Скрипт сам компилирует исходники в `out/` и запускает `darkstore.reactive.Main`, который последовательно
выполняет все три демонстрации (Observable-цепочку, имитацию живых данных, Flowable с backpressure) и
печатает лог в консоль. Отдельно компилировать или подключать зависимости вручную не нужно.

## Что демонстрируется

1. **`ObservableDemo`** — источник `Observable.fromIterable` из 10 записей (часть намеренно некорректны:
   отрицательная цена, пустое название, нулевой остаток) → `filter` (убираем нулевой остаток) → `flatMap`
   с валидацией и `map` в `ProcessedItem` (цена × количество); некорректная запись логируется и отбрасывается
   через `onErrorResumeNext`, не обрывая весь поток → `groupBy(supplierId)` + `reduce` + `toList` — суммарная
   стоимость и число позиций по каждому поставщику. `subscribeOn(Schedulers.io())` /
   `observeOn(Schedulers.computation())` — в логе видно переключение потоков (`RxCachedThreadScheduler-*` →
   `RxComputationThreadPool-*`).

2. **`LiveSimulationDemo`** — имитация поступления новых данных во времени: отдельный поток-«поставщик»
   каждые 600 мс кладёт новую запись в `PublishSubject`, которая тут же автоматически проходит ту же цепочку
   filter → flatMap(validate) → накапливается оператором `buffer(2, TimeUnit.SECONDS)` и агрегируется по
   поставщикам за это окно — без ожидания завершения потока (он в принципе бесконечный).

3. **`FlowableBackpressureDemo`** — `Flowable.create` эмитит 5000 элементов мгновенно при заведомо медленном
   подписчике (`Thread.sleep(1)` на элемент). Сначала без управления скоростью — внутренний буфер (128
   элементов) переполняется и бросается `MissingBackpressureException`, которая перехватывается в `onError`
   (приложение не падает). Затем тот же сценарий с `onBackpressureDrop()` — лишние элементы штатно
   отбрасываются, поток доходит до `onComplete` без ошибок.

## Структура

```
src/darkstore/reactive/
  Main.java                        — точка входа, запускает все три демо
  model/SupplyRecord.java          — исходная запись (supplierId, supplierName, product, price, quantity)
  model/ProcessedItem.java         — после filter+map (totalCost = price*quantity)
  model/SupplierSummary.java       — агрегат по поставщику (totalValue, itemCount)
  data/SampleDataSource.java       — тестовые наборы (initialBatch, liveBatch), включая некорректные записи
  pipeline/ObservableDemo.java
  pipeline/LiveSimulationDemo.java
  pipeline/FlowableBackpressureDemo.java
lib/
  rxjava-3.1.8.jar, reactive-streams-1.0.4.jar   — зависимости RxJava 3
bootcamp-darkstore-provider-relax1069-4h2fd/     — референс из BootcampLabs (см. выше), не изменялся
```

Для скриншотов в отчёт: вывод `run.bat` покрывает все требуемые пункты — цепочку filter/map/flatMap,
groupBy/reduce-агрегацию, обработку ошибок без падения приложения, переключение Scheduler'ов (видно по
именам потоков в квадратных скобках) и работу с backpressure во Flowable-демонстрации.
