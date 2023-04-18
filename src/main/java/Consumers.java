import com.google.gson.Gson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import javax.print.Doc;
import java.io.IOException;
import java.time.Duration;
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
  private static final String MONGO_ID = "_id";
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
  private int DB_BATCH_SIZE = 1;

  private Channel channel;
  private ConcurrentMap<String, DataStore> dataStoreMap;
  private BlockingQueue<List<WriteModel<Document>>> queue;

  private  List<WriteModel<Document>> upsertList;
  public Consumers(Connection connection, ConcurrentMap<String, DataStore> dataStoreMap,
                   BlockingQueue<List<WriteModel<Document>>> queue) {
    this.dataStoreMap = dataStoreMap;
    this.queue = queue;
    upsertList = new ArrayList<>();
//    try {
//      channel = connection.createChannel();
//      boolean durable = true;
//      channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
//
//      upsertList = new ArrayList<>();
//
//    } catch (IOException e) {
//      System.out.println(e.getMessage());
//    }
  }

  @Override
  public void run() {
    System.out.println("Started Thread: " + Thread.currentThread().getName() + " [*] Waiting for messages.");

    // RabbitMQ
//    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
//      String msg = new String(delivery.getBody(), "UTF-8");
//      String[] contents = msg.split(",");
//      Message message = new Message(contents);
//      addToDB(message);
//    };
//
//    try {
//      channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }

    Properties props = new Properties();
    props.put("group.id", "console-consumer-19543");
    props.put("bootstrap.servers", "ec2-34-216-48-154.us-west-2.compute.amazonaws.com:9092");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("auto.offset.reset", "earliest"); // set auto.offset.reset to earliest

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    consumer.subscribe(Collections.singleton("users"));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        System.out.printf("Received message: key=%s, value=%s \n", record.key(), record.value());
        Message message = deserialize(record.value());
        System.out.println(message.toString());
        addToDB(message);
      }
    }
  }

  private static Message deserialize(String json) {
    Gson gson = new Gson();
    return gson.fromJson(json, Message.class);
  }

  public void addToDB(Message message) {

    if (upsertList.size() >= DB_BATCH_SIZE) {
      List<WriteModel<Document>> tempList = new ArrayList<>(upsertList);

      queue.add(tempList);
      upsertList.clear();
    }

    if(RIGHT.equals(message.getSwipeDirection())) {
      upsertList.add(new UpdateOneModel<>(new Document(MONGO_ID, message.getSwiper()), new Document("$push", new Document(LIKES, message.getSwipee())),
              new UpdateOptions().upsert(true)));
    } else {
      upsertList.add(new UpdateOneModel<>(new Document(MONGO_ID, message.getSwiper()), new Document("$push", new Document(DISLIKES, message.getSwipee())),
              new UpdateOptions().upsert(true)));
    }
  }
}
