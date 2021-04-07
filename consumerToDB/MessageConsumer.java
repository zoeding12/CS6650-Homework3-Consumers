import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class MessageConsumer {
    private final static String EXCHANGE_NAME = "messageQueue";
    private static final DAO purchaseDao = new DAO();

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("ec2-54-90-84-139.compute-1.amazonaws.com");
        factory.setUsername("admin");
        factory.setPassword("admin");
        final Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();

        // -- non-persistent version --
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        // -- persistent version --
//        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
//        String queueName = channel.queueDeclare("", true, false, false, null).getQueue();
//        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] Thread waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            System.out.println( "Callback thread ID = " + Thread.currentThread().getId() + " Received '" + message + "'");
            processMessage(message);
        };
        // process messages
        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> { });

    }


    protected static void processMessage(String message){

        JSONObject obj = (JSONObject) JSONValue.parse(message);

        String store_id = (String) obj.get("store_id");
        String customer_id = (String) obj.get("customer_id");
        String date = (String) obj.get("date");
        // USE UUID to generate purchase_id
        String purchase_id = (String) obj.get("purchase_id");
        String items = (String) obj.get("items");

        purchaseDao.createPurchase(purchase_id, store_id, customer_id, date, items);

    }
}
