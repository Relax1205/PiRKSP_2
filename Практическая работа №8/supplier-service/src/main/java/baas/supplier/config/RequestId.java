package baas.supplier.config;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Идентификатор запроса X-Request-ID. Приходит в HTTP-заголовке или в заголовке
 * сообщения Kafka и кладётся в MDC — logback выводит его в каждой строке лога.
 */
public final class RequestId {

    public static final String HEADER = "X-Request-ID";
    public static final String MDC_KEY = "requestId";

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

    public static void putMdc(String requestId) {
        if (requestId != null) {
            MDC.put(MDC_KEY, requestId);
        }
    }

    public static void clearMdc() {
        MDC.remove(MDC_KEY);
    }
}
