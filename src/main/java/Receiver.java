import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

/**
 * @author aakash
 */
public class Receiver {

  private static final int NUM_THREADS = 10;
  private ConcurrentMap<String, DataStore> dataStoreMap;
  private ConnectionFactory factory;
  private Connection connection;

  public Receiver() {
    dataStoreMap = new ConcurrentHashMap<>();
    factory = new ConnectionFactory();
    factory.setHost("localhost");
    try {
      connection = factory.newConnection();
      } catch (IOException | TimeoutException e) {
      System.out.println(e.getMessage());
    }
  }

  public void receiveMessage() {

    Thread[] consumers = new Thread[NUM_THREADS];
    for(int i = 0; i < 10; i++) {
      Consumers consumerObject = new Consumers(connection, dataStoreMap);
      consumers[i] = new Thread(consumerObject);
      consumers[i].start();
    }
  }

  public static void main(String[] argv) {
    Receiver receiver = new Receiver();
    receiver.receiveMessage();
  }
}
