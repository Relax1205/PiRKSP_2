package baas.delivery.config;

import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Идентификатор экземпляра сервиса. В Docker hostname контейнера по умолчанию
 * совпадает с коротким CONTAINER ID из {@code docker ps}, поэтому по нему видно,
 * какая реплика обработала запрос.
 */
@Component
public class InstanceInfo {

    public record View(String hostname, String ip, String instanceId, OffsetDateTime startedAt) {
    }

    private final View view;

    public InstanceInfo() {
        String hostname;
        String ip;
        try {
            InetAddress local = InetAddress.getLocalHost();
            hostname = local.getHostName();
            ip = local.getHostAddress();
        } catch (UnknownHostException e) {
            hostname = System.getenv().getOrDefault("HOSTNAME", "unknown");
            ip = "unknown";
        }
        String instanceId = UUID.randomUUID().toString().substring(0, 8);
        this.view = new View(hostname, ip, instanceId, OffsetDateTime.now());
    }

    public View view() {
        return view;
    }
}
