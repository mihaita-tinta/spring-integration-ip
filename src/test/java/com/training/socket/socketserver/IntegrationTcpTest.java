package com.training.socket.socketserver;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.ip.IpHeaders;
import org.springframework.integration.ip.tcp.TcpReceivingChannelAdapter;
import org.springframework.integration.ip.tcp.TcpSendingMessageHandler;
import org.springframework.integration.ip.tcp.connection.TcpConnection;
import org.springframework.integration.ip.tcp.connection.TcpNioClientConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpNioServerConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpSender;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalTime;
import java.util.UUID;

import static org.springframework.integration.dsl.MessageChannels.queue;
import static org.springframework.integration.dsl.Pollers.fixedDelay;

@SpringBootTest( properties = {"app.host = localhost", "app.port = 9999"})
@RunWith(SpringRunner.class)
public class IntegrationTcpTest {
    private static final Logger log = LoggerFactory.getLogger(IntegrationTcpTest.class);


    @Test
    public void test() throws InterruptedException {
        log.info("test - start");
        Thread.sleep(10000);
    }

    @TestConfiguration
    public static class ClientIntegrationTest {


        @Bean
        public IntegrationFlow incommingFlow(TcpReceivingChannelAdapter tcpReceivingChannelAdapter) {

            return IntegrationFlows
                    .from(tcpReceivingChannelAdapter)
                    .channel(channels -> queue(1000))
                    .bridge(bridge -> bridge.poller(fixedDelay(500)))
                    .transform(source -> {
                        log.info("Transforming: {}", new String((byte[])source));
                        return source;
                    })
                    .handle(message -> log.info("handle - message: " + message))
                    .get();
        }

        @Bean
        public TcpReceivingChannelAdapter tcpReceivingChannelAdapter(@Value("${app.host}") String host,
                                                                     @Value("${app.port}") int port) {

            // Each connection factory can have only one listener (channel adapter) registered, so it's okay to
            // create it here as an 'anonomous bean' rather than 'fully blown bean'.
            TcpNioClientConnectionFactory tcpNioClientConnectionFactory = new TcpNioClientConnectionFactory(host, port);

//            tcpNioClientConnectionFactory.setDeserializer;

            TcpReceivingChannelAdapter tcpReceivingChannelAdapter = new TcpReceivingChannelAdapter();

            tcpReceivingChannelAdapter.setConnectionFactory(tcpNioClientConnectionFactory);

            tcpReceivingChannelAdapter.setAutoStartup(true); // seems to be causing re-establishment of the connection if lost
            tcpReceivingChannelAdapter.setClientMode(true); // was required to establish the connection for the first time

            // Default interval is one minute - retries are attempted both to re-establish previously available connection
            // and to establish it for the very first time, e.g. when client was started before the server.
            tcpReceivingChannelAdapter.setRetryInterval(10000);

            return tcpReceivingChannelAdapter;
        }

    }

    @TestConfiguration
    public static class ServerIntegrationTest {
        @Bean
        public IntegrationFlow integrationFlow(final TcpSendingMessageHandler messageHandler,
                                               final MessageSource<String> messageSource,
                                               TcpConnectionsTracker tcpConnectionsTracker,
                                               MessageConnectionIdHeaderPopulator messageConnectionIdHeaderPopulator) {

            return IntegrationFlows
                    .from(messageSource, endpointSpec -> endpointSpec.poller(fixedDelay(3000)))
                    .filter(message -> tcpConnectionsTracker.clientConnected())
                    .transform(messageConnectionIdHeaderPopulator)
                    .transform(message -> {System.out.println("SENDING: " + message); return message;})
                    .handle(messageHandler)
                    .get();
        }

        @Bean
        public TcpSendingMessageHandler tcpSendingMessageHandler(TcpConnectionsTracker tcpConnectionsTracker) {

            TcpSendingMessageHandler tcpSendingMessageHandler = new TcpSendingMessageHandler();
            tcpSendingMessageHandler.setConnectionFactory(tcpConnectionsTracker);
            tcpSendingMessageHandler.setClientMode(false);
            tcpSendingMessageHandler.setRetryInterval(500);
            tcpSendingMessageHandler.setLoggingEnabled(true);

            return tcpSendingMessageHandler;
        }

        @Bean
        TcpConnectionsTracker tcpConnectionsTracker(@Value("${app.port}") int port) {

            TcpConnectionsTracker tcpConnectionsTracker = new TcpConnectionsTracker(port);
            tcpConnectionsTracker.registerListener(message -> {System.out.println("MESSAGE: " + message); return true;});

            return tcpConnectionsTracker;
        }

        @Bean
        MessageConnectionIdHeaderPopulator messageConnectionIdHeaderPopulator(TcpConnectionsTracker tcpConnectionsTracker) {
            return new MessageConnectionIdHeaderPopulator(tcpConnectionsTracker);
        }

        @Bean
        public MessageSource<String> messageSource() {

            return new CsvMessageSource();
        }

    }

    static class TcpConnectionsTracker extends TcpNioServerConnectionFactory implements TcpSender {

        private TcpSender sender;
        private TcpConnection clientConnection;

        public TcpConnectionsTracker(final int port) {
            super(port);
        }

        @Override
        public void registerSender(final TcpSender sender) {
            super.registerSender(this);
            this.sender = sender;
        }

        @Override
        public void addNewConnection(final TcpConnection connection) {
            sender.addNewConnection(connection);
            clientConnection = connection;
        }

        @Override
        public void removeDeadConnection(final TcpConnection connection) {
            sender.removeDeadConnection(connection);
            clientConnection = null;
        }

        public boolean clientConnected() {
            return clientConnection != null;
        }

        public TcpConnection getClientConnection() {
            return clientConnection;
        }
    }

    static class CsvMessageSource implements MessageSource<String> {

        private int count = 0;

        @Override
        public Message<String> receive() {
            
            return MessageBuilder
                    .withPayload(String.format("%s,sample-message-%d", LocalTime.now(), count++))
                    .build();
        }
    }

    static class MessageConnectionIdHeaderPopulator implements GenericTransformer<Message, Message> {

        private TcpConnectionsTracker tcpConnectionsTracker;

        public MessageConnectionIdHeaderPopulator(
                final TcpConnectionsTracker tcpConnectionsTracker) {
            this.tcpConnectionsTracker = tcpConnectionsTracker;
        }

        @Override
        public Message transform(final Message source) {

            MessageHeaders messageHeaders = new MutableMessageHeaders(source.getHeaders());
            messageHeaders.put(IpHeaders.CONNECTION_ID, tcpConnectionsTracker.getClientConnection().getConnectionId());

            return new GenericMessage(source.getPayload(), messageHeaders);
        }
    }

}
