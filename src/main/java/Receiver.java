import com.mongodb.client.MongoClient;
import com.mongodb.client.model.WriteModel;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.bson.Document;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author aakash
 */
public class Receiver {

  private static final int NUM_THREADS = 100;
  private static final int WRITER_THREADS = 100;
  private static final String RMQ_EC2 = "35.87.18.235";
  private static final int RMQ_LB_PORT = 5672;
  private static final String LOCALHOST = "localhost";
  private static final String SWIPE_LEFT = "left";
  private static final String SWIPE_RIGHT = "right";
  private ConcurrentMap<String, DataStore> dataStoreMap;
  private ConnectionFactory factory;
  private Connection connection;
  private BlockingQueue<List<WriteModel<Document>>> queue;

  public Receiver() {
    dataStoreMap = new ConcurrentHashMap<>();
    queue = new ArrayBlockingQueue<>(500000);

//    factory = new ConnectionFactory();
//    factory.setHost(RMQ_EC2);
//    factory.setPort(RMQ_LB_PORT);
//    setUserCredentials(factory);
//    try {
//      connection = factory.newConnection();
//    } catch (IOException | TimeoutException e) {
//      System.out.println(e.getMessage());
//      System.out.println(e.getMessage());
//    }
  }


  public void receiveMessage() {

    Thread[] writers = new Thread[WRITER_THREADS];
    for(int i = 0; i < WRITER_THREADS; i++) {
      Writer writer = new Writer(queue);
      writers[i] = new Thread(writer);
      writers[i].start();
    }

    Thread[] consumers = new Thread[NUM_THREADS];
    for(int i = 0; i < NUM_THREADS; i++) {
      Consumers consumerObject = new Consumers(connection, dataStoreMap, queue);
      consumers[i] = new Thread(consumerObject);
      consumers[i].start();
    }
  }

  public void numberOfLikesAndDislikes(String userId) {
    DataStore dataStore = dataStoreMap.get(userId);
    System.out.println("The number of ppl user has swiped left on: " +
            dataStore.getSwipeStore().get(SWIPE_LEFT).size());
    System.out.println("The number of ppl user has swiped right on: " +
            dataStore.getSwipeStore().get(SWIPE_RIGHT).size());
  }

  public void listOfRightSwipedUsers(String userId) {
    DataStore dataStore = dataStoreMap.get(userId);
    int count = 0;
    for(String swipeeId : dataStore.getSwipeStore().get(SWIPE_RIGHT)) {
      if(count > 100)
        break;
      System.out.println(userId + "has swiped right on: " + swipeeId);
      count++;
    }
  }

  public void setUserCredentials(ConnectionFactory factory) {
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setVirtualHost("v1");
    factory.setPort(RMQ_LB_PORT);
  }

  public static void main(String[] argv) {


    // Mongo setup
    MongoConfig mongoConfig = MongoConfig.getInstance();
    MongoClient primaryMongoClient = mongoConfig.getMongoClient();
    Runtime.getRuntime().addShutdownHook(new Thread(primaryMongoClient::close));


    Receiver receiver = new Receiver();
    receiver.receiveMessage();
  }
}
