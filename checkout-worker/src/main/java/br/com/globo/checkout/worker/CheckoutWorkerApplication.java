package br.com.globo.checkout.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import br.com.globo.checkout.worker.application.PaymentPorts;
import br.com.globo.checkout.worker.application.PublicationCoordinator;
import br.com.globo.checkout.worker.application.ScheduledLeaseRenewer;
import br.com.globo.checkout.worker.domain.PaymentSimulator;
import br.com.globo.checkout.worker.infrastructure.KafkaCommandPublisher;
import br.com.globo.checkout.worker.infrastructure.SafeKafkaProducerListener;
import tools.jackson.databind.ObjectMapper;

@SpringBootApplication
@EnableScheduling
public class CheckoutWorkerApplication {
    @Bean PaymentSimulator paymentSimulator() { return new PaymentSimulator(); }

    @Bean(destroyMethod = "shutdownNow") ThreadPoolExecutor kafkaSendExecutor() {
        return boundedExecutor("checkout-v2-kafka-send");
    }

    @Bean(destroyMethod = "shutdownNow") ThreadPoolExecutor failureSendExecutor() {
        return boundedExecutor("checkout-v2-kafka-failure-send");
    }

    @Bean(destroyMethod = "shutdownNow") ThreadPoolExecutor recoveryExecutor() {
        return new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), task -> {
            Thread thread = new Thread(task, "checkout-v2-publication-recovery");
            thread.setDaemon(true);
            return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
    }

    @Bean PaymentPorts.CommandPublisher commandPublisher(KafkaTemplate<String, String> kafka,
            ObjectMapper mapper, ThreadPoolExecutor kafkaSendExecutor,
            @Value("${checkout.kafka.command-topic:checkout-v2-commands}") String topic) {
        kafka.setProducerListener(new SafeKafkaProducerListener());
        return new KafkaCommandPublisher(kafka, mapper, kafkaSendExecutor, topic);
    }

    @Bean PublicationCoordinator publicationCoordinator(PaymentPorts.AttemptStore attempts,
            PaymentPorts.CommandPublisher publisher) {
        return new PublicationCoordinator(attempts, publisher);
    }

    @Bean(destroyMethod = "shutdownNow") ScheduledExecutorService leaseRenewalScheduler() {
        return Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "checkout-v2-lease-renewal");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Bean ScheduledLeaseRenewer scheduledLeaseRenewer(PaymentPorts.AttemptStore attempts,
            ScheduledExecutorService leaseRenewalScheduler,
            @Value("${checkout.kafka.lease-renewal-interval-ms:10000}") long renewalIntervalMillis) {
        return new ScheduledLeaseRenewer(attempts, leaseRenewalScheduler, renewalIntervalMillis);
    }

    public static void main(String[] args) {
        boolean cli = args.length > 0 && !args[0].startsWith("--");
        if (!cli) {
            SpringApplication.run(CheckoutWorkerApplication.class, args);
            return;
        }
        var application = new SpringApplication(CheckoutWorkerApplication.class);
        application.setAdditionalProfiles("worker-cli");
        var context = application.run(args);
        int result = context.getBean(br.com.globo.checkout.worker.infrastructure.WorkerCli.class).execute(args);
        SpringApplication.exit(context);
        if (result != 0) System.exit(result);
    }

    private static ThreadPoolExecutor boundedExecutor(String threadName) {
        return new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), task -> {
            Thread thread = new Thread(task, threadName);
            thread.setDaemon(true);
            return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
    }
}
