package baas.orders.config;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Компонент kafka в /actuator/health: доступен ли брокер. Проверка укладывается в ~1 с,
 * чтобы health сервиса отвечал быстро, даже когда Kafka недоступна.
 */
@Component
public class KafkaHealthIndicator implements HealthIndicator {

    private final AdminClient adminClient;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1500);
        this.adminClient = AdminClient.create(config);
    }

    @Override
    public Health health() {
        try {
            DescribeClusterResult cluster = adminClient.describeCluster(new DescribeClusterOptions().timeoutMs(1000));
            return Health.up()
                    .withDetail("clusterId", cluster.clusterId().get(1500, TimeUnit.MILLISECONDS))
                    .withDetail("brokers", cluster.nodes().get(1500, TimeUnit.MILLISECONDS).size())
                    .build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Health.down(e).build();
        } catch (Exception e) {
            return Health.down().withDetail("error", "Kafka недоступна: " + e.getClass().getSimpleName()).build();
        }
    }

    @PreDestroy
    public void close() {
        adminClient.close();
    }
}
