import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.bson.Document;

import javax.print.Doc;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

/**
 * @author aakash
 */
public class Consumers implements Runnable {
  //private static final String QUEUE_NAME = "twinder_queue";
  private static final String QUEUE_NAME = "LIKE";

  //private static String MONGO_ID = "_id";
  private static final String MONGO_ID = "SwiperID";
  private static String NUM_LIKES = "numLikes";
  private static String NUM_DISLIKES = "numDislikes";
  private static String LIKES = "likes";
  private static String DISLIKES = "dislikes";
  private static String SWIPE_DB = "swipe";
  private static String USER_ID = "userId";
  private static String LEFT = "left";
  private static String RIGHT = "right";
  private static String RIGHT_SWIPED = "rightSwiped";
  private static String MATCH_LIST = "matchList";
  private static String COMMENTS = "comments";
  private int DB_BATCH_SIZE = 1000;

  private Channel channel;
  private ConcurrentMap<String, DataStore> dataStoreMap;
  private BlockingQueue<List<WriteModel<Document>>> queue;

  private  List<WriteModel<Document>> upsertList;
  public Consumers(Connection connection, ConcurrentMap<String, DataStore> dataStoreMap,
                   BlockingQueue<List<WriteModel<Document>>> queue) {
    this.dataStoreMap = dataStoreMap;
    this.queue = queue;
    try {
      channel = connection.createChannel();
      boolean durable = true;
      channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

      upsertList = new ArrayList<>();

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
      addToDB(message);
    };

    try {
      channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void addToDB(Message message) {

    if (upsertList.size() >= DB_BATCH_SIZE) {
      List<WriteModel<Document>> tempList = new ArrayList<>(upsertList);

      queue.add(tempList);
      upsertList.clear();
    }

    if(RIGHT.equals(message.getSwipeDirection())) {
      upsertList.add(new UpdateOneModel<>(new Document(MONGO_ID, message.getSwiperId()), new Document("$push", new Document(LIKES, message.getSwipeeId())),
              new UpdateOptions().upsert(true)));
    } else {
      upsertList.add(new UpdateOneModel<>(new Document(MONGO_ID, message.getSwiperId()), new Document("$push", new Document(DISLIKES, message.getSwipeeId())),
              new UpdateOptions().upsert(true)));
    }
  }
}
