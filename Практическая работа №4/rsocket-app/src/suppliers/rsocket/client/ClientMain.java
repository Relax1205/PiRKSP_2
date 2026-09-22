package suppliers.rsocket.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import suppliers.rsocket.domain.OrderEvent;
import suppliers.rsocket.domain.Product;
import suppliers.rsocket.domain.Supplier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Клиент RSocket, демонстрирующий четыре модели взаимодействия
 * (Request-Response, Request-Stream, Fire-and-Forget, Channel)
 * с реактивной обработкой ответов средствами Project Reactor.
 */
public class ClientMain {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws InterruptedException {
        RSocket socket = RSocketConnector.create()
                .connect(TcpClientTransport.create("localhost", 7000))
                .block();

        System.out.println("Клиент подключился к RSocket-серверу АС Поставщиков (localhost:7000)\n");

        demoRequestResponse(socket);
        demoRequestStream(socket);
        demoFireAndForget(socket);
        demoChannel(socket);

        socket.dispose();
        System.out.println("\nСоединение закрыто.");
    }

    // ---------- 1. Request-Response ----------
    private static void demoRequestResponse(RSocket socket) {
        System.out.println("== 1) Request-Response: получить товар по id ==");

        Product product = socket.requestResponse(payload("product.get", "1"))
                .map(ClientMain::readProduct)
                .doOnNext(p -> System.out.println("Получен товар: " + p))
                .block();

        System.out.println("== Request-Response: получить поставщика по id ==");
        Supplier supplier = socket.requestResponse(payload("supplier.get", "2"))
                .map(p -> readValue(p, Supplier.class))
                .doOnNext(s -> System.out.println("Получен поставщик: " + s))
                .block();

        System.out.println("== Request-Response: запрос несуществующего товара (обработка ошибки) ==");
        socket.requestResponse(payload("product.get", "999"))
                .map(ClientMain::readProduct)
                .doOnNext(p -> System.out.println("Не должно быть достигнуто: " + p))
                .onErrorResume(e -> {
                    System.out.println("Ожидаемая ошибка сервера: " + e.getMessage());
                    return Mono.empty();
                })
                .block();

        System.out.println();
    }

    // ---------- 2. Request-Stream ----------
    private static void demoRequestStream(RSocket socket) {
        System.out.println("== 2) Request-Stream: получить каталог товаров потоком ==");

        Flux<Product> stream = socket.requestStream(payload("product.stream", ""))
                .map(ClientMain::readProduct);

        stream
                .doOnSubscribe(s -> System.out.println("Подписка оформлена, элементы будут приходить постепенно:"))
                .doOnNext(p -> System.out.println("[" + LocalTime.now().withNano(0) + "] Товар №"
                        + p.getId() + " \"" + p.getName() + "\" — " + (long) p.getPrice() + " ₽ (остаток: "
                        + p.getQuantity() + " шт.)"))
                .doOnComplete(() -> System.out.println("Поток товаров завершён."))
                .blockLast();

        System.out.println();
    }

    // ---------- 3. Fire-and-Forget ----------
    private static void demoFireAndForget(RSocket socket) {
        System.out.println("== 3) Fire-and-Forget: отправка события о новом заказе без ожидания ответа ==");

        OrderEvent event = new OrderEvent(2, 3, "Срочный заказ от клиента через RSocket");
        try {
            String json = MAPPER.writeValueAsString(event);
            socket.fireAndForget(payload("order.create", json))
                    .doOnSuccess(v -> System.out.println("Событие отправлено на сервер (ответ не ожидается): " + event))
                    .block();
        } catch (Exception e) {
            System.out.println("Не удалось сериализовать событие: " + e.getMessage());
        }

        System.out.println();
    }

    // ---------- 4. Channel ----------
    private static void demoChannel(RSocket socket) throws InterruptedException {
        System.out.println("== 4) Channel: двусторонний обмен — отслеживание статусов заказов ==");

        // Клиент постепенно отправляет id заказов, которые хочет отслеживать
        // (первый запрос содержит id=1 -> существующий заказ, второй id=999 -> несуществующий).
        Flux<Payload> requests = Flux.concat(
                Mono.just(payload("order.track", "1")),
                Mono.just(payload("", "999")).delayElement(Duration.ofMillis(200)),
                Mono.just(payload("", "2")).delayElement(Duration.ofMillis(200))
        );

        CountDownLatch latch = new CountDownLatch(1);

        socket.requestChannel(requests)
                .doOnNext(p -> {
                    String json = p.getDataUtf8();
                    p.release();
                    System.out.println("[" + LocalTime.now().withNano(0) + "] Обновление из канала: " + json);
                })
                .doOnComplete(() -> System.out.println("Канал отслеживания заказов закрыт."))
                .doOnError(e -> System.out.println("Ошибка канала: " + e.getMessage()))
                .doFinally(sig -> latch.countDown())
                .subscribe();

        latch.await(10, TimeUnit.SECONDS);
        System.out.println();
    }

    private static Payload payload(String route, String data) {
        return DefaultPayload.create(
                data.getBytes(StandardCharsets.UTF_8),
                route.getBytes(StandardCharsets.UTF_8));
    }

    private static Product readProduct(Payload payload) {
        return readValue(payload, Product.class);
    }

    private static <T> T readValue(Payload payload, Class<T> type) {
        try {
            String json = payload.getDataUtf8();
            payload.release();
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new RuntimeException("Ошибка разбора ответа сервера", e);
        }
    }
}
