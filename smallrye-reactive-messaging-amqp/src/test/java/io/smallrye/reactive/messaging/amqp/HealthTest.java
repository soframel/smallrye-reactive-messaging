package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.common.constraint.Assert.assertFalse;
import static io.smallrye.common.constraint.Assert.assertTrue;
import static org.awaitility.Awaitility.await;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.health.HealthReport;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.*;
import org.jboss.weld.bean.ManagedBean;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Flow;

public class HealthTest extends AmqpBrokerTestBase {
    private WeldContainer container;
    private final Weld weld = new Weld();

    @AfterEach
    public void cleanup() {
        if (container != null) {
            container.shutdown();
        }

        MapBasedConfig.cleanup();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testReadinessAndLivenessEnabledProducer() {
        String addressSink = UUID.randomUUID().toString();
        String addressSource = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.address", addressSink)
                .with("mp.messaging.outgoing.sink.host", host)
                .with("mp.messaging.outgoing.sink.port", port)
                .write();

        weld.addBeanClass(ProducingBean.class);
        container = weld.initialize();

        await().until(() -> isAmqpConnectorAlive(container));
        await().until(() -> isAmqpConnectorReady(container));
    }
    @Test
    public void testReadinessAndLivenessEnabledConsumer() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("mp.messaging.incoming.data.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.address", address)
                .with("mp.messaging.incoming.data.host", host)
                .with("mp.messaging.incoming.data.port", port)
                .write();

        weld.addBeanClass(ConsumptionBean.class);
        container = weld.initialize();
        await().until(() -> isAmqpConnectorReady(container));
        await().until(() -> isAmqpConnectorAlive(container));
    }

    @Test
    public void testHealthDisabledProducer() {
        String address = UUID.randomUUID().toString();

        new MapBasedConfig()
                .with("amqp-username", username)
                .with("amqp-password", password)
                .with("mp.messaging.outgoing.sink.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.address", address)
                .with("mp.messaging.outgoing.sink.host", host)
                .with("mp.messaging.outgoing.sink.port", port)
                .with("health-enabled", false)
                .write();

        weld.addBeanClass(ProducingBean.class);
        container = weld.initialize();

        HealthCenter health=this.getHealthCenter();
        assertTrue(health.getLiveness().isOk());
        List<HealthReport.ChannelInfo> livenessChannels=health.getLiveness().getChannels();

        assertFalse(livenessChannels.stream()
                .map(HealthReport.ChannelInfo::getChannel)
                .anyMatch(s -> s.equals("SmallRye Reactive Messaging - liveness check")));
        assertFalse(livenessChannels.stream()
                .map(HealthReport.ChannelInfo::getChannel)
                .anyMatch(s -> s.equals("SmallRye Reactive Messaging - readiness check")));
        assertFalse(livenessChannels.stream()
                .map(HealthReport.ChannelInfo::getChannel)
                .anyMatch(s -> s.equals("SmallRye Reactive Messaging - startup check")));
    }

    private HealthCenter getHealthCenter(){
        return container.getBeanManager().createInstance().select(HealthCenter.class).get();
    }



  /*  @Test
    void testReadinessAndLivenessEnabled() {
        MapBasedConfig config = getBaseConfig();

        addBeans(MyApp.class);
        runApplication(config);
        HealthCenter health = get(container, HealthCenter.class);
        assertThat(health.getLiveness().isOk()).isTrue();
        assertThat(health.getLiveness().getChannels()).anySatisfy(ci -> {
            assertThat(ci.getChannel()).isEqualTo("in");
            assertThat(ci.isOk()).isTrue();
        })
                .anySatisfy(ci -> {
                    assertThat(ci.getChannel()).isEqualTo("out");
                    assertThat(ci.isOk()).isTrue();
                });

        assertThat(health.getReadiness().isOk()).isTrue();
        assertThat(health.getReadiness().getChannels()).anySatisfy(ci -> {
            assertThat(ci.getChannel()).isEqualTo("in");
            assertThat(ci.isOk()).isTrue();
        })
                .anySatisfy(ci -> {
                    assertThat(ci.getChannel()).isEqualTo("out");
                    assertThat(ci.isOk()).isTrue();
                });
    }

    @Test
    void testHealthDisabled() {
        MapBasedConfig config = getBaseConfig()
                .with("mp.messaging.incoming.in.health-enabled", false)
                .with("mp.messaging.outgoing.out.health-enabled", false);

        addBeans(MyApp.class);
        runApplication(config);
        HealthCenter health = get(container, HealthCenter.class);
        assertThat(health.getLiveness().isOk()).isTrue();
        assertThat(health.getLiveness().getChannels()).isEmpty();

        assertThat(health.getReadiness().isOk()).isTrue();
        assertThat(health.getReadiness().getChannels()).isEmpty();
    }

    @Test
    void testReadinessDisabled() {
        MapBasedConfig config = getBaseConfig()
                .with("mp.messaging.incoming.in.health-readiness-enabled", false)
                .with("mp.messaging.outgoing.out.health-readiness-enabled", false);

        addBeans(MyApp.class);
        runApplication(config);
        HealthCenter health = get(container, HealthCenter.class);
        assertThat(health.getLiveness().isOk()).isTrue();
        assertThat(health.getLiveness().getChannels()).hasSize(2);

        assertThat(health.getReadiness().isOk()).isTrue();
        assertThat(health.getReadiness().getChannels()).isEmpty();
    }

    @Test
    void testWithAppUsingChannels() {
        MapBasedConfig config = getBaseConfig()
                .with("mp.messaging.incoming.in.health-lazy-subscription", true);

        addBeans(MyAppUsingChannels.class);
        runApplication(config);
        HealthCenter health = get(container, HealthCenter.class);
        assertThat(health.getLiveness().isOk()).isTrue();
        assertThat(health.getLiveness().getChannels()).anySatisfy(ci -> {
            assertThat(ci.getChannel()).isEqualTo("in");
            assertThat(ci.isOk()).isTrue();
        })
                .anySatisfy(ci -> {
                    assertThat(ci.getChannel()).isEqualTo("out");
                    assertThat(ci.isOk()).isTrue();
                });

        assertThat(health.getReadiness().isOk()).isTrue();
        assertThat(health.getReadiness().getChannels()).anySatisfy(ci -> {
            assertThat(ci.getChannel()).isEqualTo("in");
            assertThat(ci.isOk()).isTrue();
        })
                .anySatisfy(ci -> {
                    assertThat(ci.getChannel()).isEqualTo("out");
                    assertThat(ci.isOk()).isTrue();
                });
    }*/

    public static class MyApp {
        @Incoming("in")
        @Outgoing("out")
        public String process(String in) {
            return in;
        }
    }

    public static class MyAppUsingChannels {

        @Inject
        @Channel("in")
        Multi<String> in;

        @Inject
        @Channel("out")
        Emitter<String> out;

    }
}
