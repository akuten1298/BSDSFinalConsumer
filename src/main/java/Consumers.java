import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * @author aakash
 */
public class Consumers implements Runnable {
  private static final String QUEUE_NAME = "twinder_queue";
  private Channel channel;
  private ConcurrentMap<String, DataStore> dataStoreMap;
  public Consumers(Connection connection, ConcurrentMap<String, DataStore> dataStoreMap) {
    this.dataStoreMap = dataStoreMap;
    try {
      channel = connection.createChannel();
      channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

  @Override
  public void run() {
    System.out.println("Started Thread: " + Thread.currentThread().getName() + " [*] Waiting for messages.");

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String msg = new String(delivery.getBody(), "UTF-8");
      String[] contents = msg.split(",");
      Message message = new Message(contents);

      DataStore dataStore = dataStoreMap.get(message.getSwiperId());
      if(dataStore == null)
        dataStore = new DataStore(message.getSwiperId());

      dataStore.updateStore(message.getSwipeDirection(), message.getSwipeeId());

      dataStoreMap.put(message.getSwiperId(), dataStore);
    };

    try {
      channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
