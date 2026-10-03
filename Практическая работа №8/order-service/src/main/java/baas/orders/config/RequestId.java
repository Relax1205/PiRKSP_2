package baas.orders.config;

import io.micrometer.context.ContextRegistry;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Идентификатор запроса X-Request-ID. В WebFlux один запрос обрабатывают разные потоки,
 * поэтому значение хранится не в ThreadLocal, а в Reactor Context цепочки. Оттуда
 * Reactor сам копирует его в MDC (для логов) на каждом потоке.
 */
public final class RequestId {

    public static final String HEADER = "X-Request-ID";
    public static final String MDC_KEY = "requestId";
    public static final String CONTEXT_KEY = "requestId";

    /** Только безопасные символы: значение попадает в логи. */
    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9._:-]{1,64}");

    private RequestId() {
    }

    /** Значение из заголовка или новое, если заголовка нет или он некорректный. */
    public static String normalize(String candidate) {
        return candidate != null && VALID.matcher(candidate).matches() ? candidate : generate();
    }

    public static String generate() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    public static String fromKafka(Headers headers) {
        Header header = headers.lastHeader(HEADER);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    public static Context write(Context context, String requestId) {
        return requestId == null ? context : context.put(CONTEXT_KEY, requestId);
    }

    public static void putMdc(String requestId) {
        if (requestId != null) {
            MDC.put(MDC_KEY, requestId);
        }
    }

    public static void clearMdc() {
        MDC.remove(MDC_KEY);
    }

    /**
     * MDC регистрируется как ThreadLocal, который Reactor восстанавливает из Context
     * (spring.reactor.context-propagation=auto). Вызывается один раз при старте.
     */
    public static void registerMdcPropagation() {
        ContextRegistry.getInstance().registerThreadLocalAccessor(CONTEXT_KEY,
                () -> MDC.get(MDC_KEY),
                value -> MDC.put(MDC_KEY, value),
                () -> MDC.remove(MDC_KEY));
    }

    /** Фильтр WebClient: X-Request-ID текущего запроса уходит дальше в Supplier Service. */
    public static ExchangeFilterFunction propagate() {
        return (request, next) -> Mono.deferContextual(ctx -> next.exchange(
                ctx.<String>getOrEmpty(CONTEXT_KEY)
                        .map(id -> ClientRequest.from(request).header(HEADER, id).build())
                        .orElse(request)));
    }
}
