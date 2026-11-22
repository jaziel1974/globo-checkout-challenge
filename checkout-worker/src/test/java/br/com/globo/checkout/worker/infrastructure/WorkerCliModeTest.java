package br.com.globo.checkout.worker.infrastructure;

import static org.junit.jupiter.api.Assertions.*;

import br.com.globo.checkout.worker.CheckoutWorkerApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.scheduling.config.ScheduledTaskHolder;

@SpringBootTest(classes = CheckoutWorkerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {"spring.kafka.admin.auto-create=false", "spring.data.redis.host=127.0.0.1", "spring.data.redis.port=6379"})
@ActiveProfiles("worker-cli")
class WorkerCliModeTest {
    @Autowired WorkerCli cli;
    @Autowired KafkaListenerEndpointRegistry listeners;
    @Autowired ScheduledTaskHolder scheduledTasks;

    @Test void typoIsAnExitTwoUsageErrorAndCliContextStartsNoListenerOrScheduler() {
        assertEquals(2, cli.execute(new String[]{"inspec", "--attempt-id", "not-a-valid-uuid"}));
        assertTrue(listeners.getListenerContainers().isEmpty());
        assertTrue(scheduledTasks.getScheduledTasks().isEmpty());
    }
}
