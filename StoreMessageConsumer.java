package runConsumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StoreMessageConsumer {
    private static final String EXCHANGE_NAME = "messageQueue";
    private final static Jedis jedis = new Jedis("localhost");

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("ec2-54-90-84-139.compute-1.amazonaws.com");
        factory.setUsername("admin");
        factory.setPassword("admin");
        Connection connection = factory.newConnection();
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
            storeToRedis(message);
        };
        // process messages
        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> { });
    }


    private static void storeToRedis(String message){

        JSONObject obj = (JSONObject) JSONValue.parse(message);

        String store_id = (String) obj.get("store_id");

        // add up the items of each purchase
        JSONObject itemsObj = (JSONObject) JSONValue.parse((String) obj.get("items"));
        JSONArray itemsArray = (JSONArray) itemsObj.get("items");
        Map<String, Integer> itemsCount = new HashMap<>();
        for(Object o : itemsArray){
            JSONObject item = (JSONObject) JSONValue.parse(o.toString());
            String key = (String) item.get("ItemID");
            int num = ((Long) item.get("numberOfItems:")).intValue();
            itemsCount.put(key, itemsCount.getOrDefault(key, 0) + num);
        }

        String storeKey = "store_" + store_id;
        // store_id: {item_id : count}
        if(jedis.exists(storeKey)){
            for(Map.Entry<String, Integer> entry : itemsCount.entrySet()){
                jedis.hincrBy(storeKey, entry.getKey(), entry.getValue());
            }
        }else{
            for(Map.Entry<String, Integer> entry : itemsCount.entrySet()){
                jedis.hset(storeKey, entry.getKey(), entry.getValue().toString());
            }
        }

        // item_id: {store_id : count}
        for(Map.Entry<String, Integer> entry : itemsCount.entrySet()){
            String itemKey = entry.getKey();
            if(jedis.exists(itemKey)){
                jedis.hincrBy(itemKey, store_id, entry.getValue());
            }else{
                jedis.hset(itemKey, store_id, entry.getValue().toString());
            }
        }


    }
}
