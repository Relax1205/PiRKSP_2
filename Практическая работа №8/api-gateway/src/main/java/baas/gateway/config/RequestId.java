package baas.gateway.config;

import io.micrometer.context.ContextRegistry;
import org.slf4j.MDC;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Идентификатор запроса X-Request-ID. API Gateway — первая точка, где он появляется:
 * берётся из запроса клиента или генерируется, а затем передаётся во все сервисы.
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

    /** Выполнить действие (запись в лог) с requestId в MDC на текущем потоке. */
    public static void withMdc(String requestId, Runnable action) {
        if (requestId == null) {
            action.run();
            return;
        }
        String previous = MDC.get(MDC_KEY);
        MDC.put(MDC_KEY, requestId);
        try {
            action.run();
        } finally {
            if (previous == null) {
                MDC.remove(MDC_KEY);
            } else {
                MDC.put(MDC_KEY, previous);
            }
        }
    }
}
