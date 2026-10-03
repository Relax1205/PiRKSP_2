package baas.gateway.config;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.netty.channel.ConnectTimeoutException;
import org.springframework.core.NestedExceptionUtils;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

/** Понятное описание, почему сервис недоступен — для fallback-ответов и health. */
public final class Failures {

    private Failures() {
    }

    public static String describe(Throwable e) {
        if (e == null) {
            return "нет ответа";
        }
        if (e instanceof CallNotPermittedException) {
            return "circuit breaker API Gateway открыт, запросы к сервису временно не отправляются";
        }
        if (e instanceof TimeoutException || e instanceof org.springframework.cloud.gateway.support.TimeoutException) {
            return "сервис не ответил вовремя (таймаут ответа)";
        }
        // порядок важен: ConnectTimeoutException — подкласс ConnectException
        if (hasCause(e, ConnectTimeoutException.class)) {
            return "сервис не отвечает (таймаут подключения)";
        }
        if (hasCause(e, UnknownHostException.class)) {
            return "сервис не найден в сети (контейнер остановлен?)";
        }
        if (hasCause(e, ConnectException.class)) {
            return "соединение отклонено";
        }
        Throwable root = NestedExceptionUtils.getMostSpecificCause(e);
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    private static boolean hasCause(Throwable e, Class<? extends Throwable> type) {
        for (Throwable current = e; current != null; current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }
}
