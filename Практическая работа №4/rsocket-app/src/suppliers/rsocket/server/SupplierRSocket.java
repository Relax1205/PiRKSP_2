package suppliers.rsocket.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import suppliers.rsocket.domain.Order;
import suppliers.rsocket.domain.OrderEvent;
import suppliers.rsocket.domain.Product;

import java.time.Duration;

/**
 * RSocket-реализация серверной части АС Поставщиков.
 *
 * Маршрут (route) операции передаётся в метаданных payload'а простой UTF-8 строкой,
 * а сами данные сериализуются в JSON (Jackson).
 *
 * Поддерживаемые маршруты:
 *  - "product.get"    (Request-Response) — получить товар по id;
 *  - "product.stream"  (Request-Stream)   — получить поток всех товаров с задержкой;
 *  - "order.create"    (Fire-and-Forget)  — создать заказ без ожидания ответа;
 *  - "order.track"     (Channel)          — двусторонний обмен: клиент шлёт id заказов,
 *                                            сервер шлёт поток обновлений статуса по каждому.
 */
public class SupplierRSocket implements RSocket {

    private final DataStore store;
    private final ObjectMapper mapper = new ObjectMapper();

    public SupplierRSocket(DataStore store) {
        this.store = store;
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        String route;
        String data;
        try {
            route = payload.getMetadataUtf8();
            data = payload.getDataUtf8();
        } finally {
            payload.release();
        }
        return Mono.defer(() -> handleRequestResponse(route, data));
    }

    private Mono<Payload> handleRequestResponse(String route, String data) {
        if ("product.get".equals(route)) {
            return parseId(data)
                    .flatMap(id -> {
                        Product product = store.getProduct(id);
                        if (product == null) {
                            return Mono.error(new ApplicationErrorException(
                                    "Товар с id=" + id + " не найден"));
                        }
                        System.out.println("[Request-Response] Отдан товар: " + product);
                        return Mono.just(toPayload(product));
                    });
        }
        if ("supplier.get".equals(route)) {
            return parseId(data)
                    .flatMap(id -> {
                        var supplier = store.getSupplier(id);
                        if (supplier == null) {
                            return Mono.error(new ApplicationErrorException(
                                    "Поставщик с id=" + id + " не найден"));
                        }
                        System.out.println("[Request-Response] Отдан поставщик: " + supplier);
                        return Mono.just(toPayload(supplier));
                    });
        }
        return Mono.error(new ApplicationErrorException("Неизвестный маршрут: " + route));
    }

    private Mono<Integer> parseId(String data) {
        try {
            return Mono.just(Integer.parseInt(data.trim()));
        } catch (NumberFormatException e) {
            return Mono.error(new ApplicationErrorException("Идентификатор должен быть числом: '" + data + "'"));
        }
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        String route;
        try {
            route = payload.getMetadataUtf8();
        } finally {
            payload.release();
        }
        if (!"product.stream".equals(route)) {
            return Flux.error(new ApplicationErrorException("Неизвестный маршрут: " + route));
        }
        System.out.println("[Request-Stream] Начата передача каталога товаров...");
        return Flux.fromIterable(store.allProducts())
                .delayElements(Duration.ofMillis(700))
                .doOnNext(p -> System.out.println("[Request-Stream] -> " + p.getName() + " — " + (long) p.getPrice() + " руб."))
                .map(this::toPayload);
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        String route;
        String data;
        try {
            route = payload.getMetadataUtf8();
            data = payload.getDataUtf8();
        } finally {
            payload.release();
        }
        return Mono.fromRunnable(() -> {
            if (!"order.create".equals(route)) {
                System.err.println("[Fire-and-Forget] Неизвестный маршрут: " + route);
                return;
            }
            try {
                OrderEvent event = mapper.readValue(data, OrderEvent.class);
                Order order = store.createOrder(event.getProductId(), event.getQuantity());
                System.out.println("[Fire-and-Forget] Получено событие заказа (ответ клиенту не отправляется): "
                        + event + " -> создан заказ " + order);
            } catch (Exception e) {
                System.err.println("[Fire-and-Forget] Ошибка обработки события: " + e.getMessage());
            }
        });
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        System.out.println("[Channel] Клиент открыл канал отслеживания заказов");
        return Flux.from(payloads)
                .flatMap(this::handleChannelRequest);
    }

    private Flux<Payload> handleChannelRequest(Payload payload) {
        String route;
        String data;
        try {
            route = payload.getMetadataUtf8();
            data = payload.getDataUtf8();
        } finally {
            payload.release();
        }
        String effectiveRoute = (route == null || route.isEmpty()) ? "order.track" : route;
        if (!"order.track".equals(effectiveRoute)) {
            return Flux.just(toErrorPayload("Неизвестный маршрут канала: " + effectiveRoute));
        }

        int orderId;
        try {
            orderId = Integer.parseInt(data.trim());
        } catch (NumberFormatException e) {
            return Flux.just(toErrorPayload("Идентификатор заказа должен быть числом: '" + data + "'"));
        }

        if (store.getOrder(orderId) == null) {
            return Flux.just(toErrorPayload("Заказ с id=" + orderId + " не найден"));
        }

        System.out.println("[Channel] Получен запрос отслеживания заказа id=" + orderId);
        return Flux.range(0, 3)
                .delayElements(Duration.ofMillis(600))
                .map(i -> store.advanceOrderStatus(orderId))
                .doOnNext(o -> System.out.println("[Channel] -> обновление статуса: " + o))
                .map(this::toPayload);
    }

    private Payload toPayload(Object value) {
        try {
            return DefaultPayload.create(mapper.writeValueAsBytes(value));
        } catch (Exception e) {
            throw new RuntimeException("Ошибка сериализации ответа", e);
        }
    }

    private Payload toErrorPayload(String message) {
        try {
            return DefaultPayload.create(mapper.writeValueAsBytes(new ErrorBody(message)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Тело сообщения об ошибке внутри Channel-потока (не прерывает весь поток). */
    public static class ErrorBody {
        public String error;

        public ErrorBody() {
        }

        public ErrorBody(String error) {
            this.error = error;
        }
    }
}
