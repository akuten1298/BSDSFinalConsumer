import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.bson.Document;

import javax.print.Doc;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

/**
 * @author aakash
 */
public class Consumers implements Runnable {
  private static MongoCollection<Document> statsCollection;
  private static MongoCollection<Document> matchesCollection;
  private static MongoCollection<Document> swipeCollection;

  private static final String QUEUE_NAME = "twinder_queue";
  private static String MONGO_ID = "_id";
  private static String NUM_LIKES = "numLikes";
  private static String NUM_DISLIKES = "numDislikes";
  private static String LIKES = "likes";
  private static String DISLIKES = "dislikes";
  private static String STATS_DB = "stats";
  private static String MATCHES_DB = "matches";
  private static String SWIPE_DB = "swipe";
  private static String USER_ID = "userId";
  private static String LEFT = "left";
  private static String RIGHT = "right";
  private static String RIGHT_SWIPED = "rightSwiped";
  private static String MATCH_LIST = "matchList";
  private static String COMMENTS = "comments";

  private Channel channel;
  private ConcurrentMap<String, DataStore> dataStoreMap;
  public Consumers(Connection connection, ConcurrentMap<String, DataStore> dataStoreMap) {
    this.dataStoreMap = dataStoreMap;
    try {
      channel = connection.createChannel();
      boolean durable = true;
      channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

      MongoConfig mongoConfig = MongoConfig.getInstance();
      MongoDatabase database = mongoConfig.getDatabase();

      statsCollection = database.getCollection(STATS_DB);
      matchesCollection = database.getCollection(MATCHES_DB);
      swipeCollection = database.getCollection(SWIPE_DB);

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

//      DataStore dataStore = dataStoreMap.get(message.getSwiperId());
//      if(dataStore == null)
//        dataStore = new DataStore(message.getSwiperId());
//
//      dataStore.updateStore(message.getSwipeDirection(), message.getSwipeeId());
//
//      dataStoreMap.put(message.getSwiperId(), dataStore);

//      updateStats(message);
//      updateMatches(message);
      addToDB(message);
    };

    try {
      channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void addToDB(Message message) {
    Document swiperDoc = swipeCollection.find(eq(MONGO_ID, message.getSwiperId())).first();
    if (swiperDoc == null) {
      swiperDoc = new Document(MONGO_ID, message.getSwiperId());
      Set<String> likesSet = new HashSet<>();
      Set<String> disLikesSet = new HashSet<>();

      if(RIGHT.equals(message.getSwipeDirection())) {
        likesSet.add(message.getSwipeeId());
      } else {
        disLikesSet.add(message.getSwipeeId());
      }

      swiperDoc.put(LIKES, likesSet);
      swiperDoc.put(DISLIKES, disLikesSet);
      swipeCollection.insertOne(swiperDoc);
    } else {
      Set<String> subDocument = new HashSet<>((Collection) (RIGHT.equals(message.getSwipeDirection()) ?
                          swiperDoc.get(LIKES) : swiperDoc.get(DISLIKES)));
      subDocument.add(message.getSwipeeId());
      swipeCollection.updateOne(swiperDoc, set(RIGHT.equals(message.getSwipeDirection()) ?
              LIKES : DISLIKES, subDocument));
    }
  }

  public void updateStats(Message message) {
    Document myDoc = statsCollection.find(eq(USER_ID, message.getSwipeeId())).first();
    System.out.println("My Doc: " + myDoc);
    if(myDoc == null) {
      System.out.println("Inside insertion");
      myDoc = new Document(USER_ID, message.getSwipeeId());
      if(LEFT.equals(message.getSwipeDirection())) {
        myDoc.put(NUM_DISLIKES, 1);
        myDoc.put(NUM_LIKES, 0);
      } else {
        myDoc.put(NUM_DISLIKES, 0);
        myDoc.put(NUM_LIKES, 1);
      }
      statsCollection.insertOne(myDoc);
    } else {
      if(LEFT.equals(message.getSwipeDirection())) {
        UpdateResult updateResult =
                statsCollection.updateOne(myDoc, set(NUM_DISLIKES, myDoc.getInteger(NUM_DISLIKES) + 1));
      } else {
        UpdateResult updateResult =
                statsCollection.updateOne(myDoc, set(NUM_LIKES, myDoc.getInteger(NUM_LIKES) + 1));
      }
    }
  }

  public void updateMatches(Message message) {
    Document swiperDoc = matchesCollection.find(eq(USER_ID, message.getSwiperId())).first();
    if(swiperDoc == null) {
      swiperDoc = new Document(USER_ID, message.getSwiperId());
      Set<String> sets = new HashSet<>();
      if(RIGHT.equals(message.getSwipeDirection()))
        sets.add(message.getSwipeeId());
      swiperDoc.put(RIGHT_SWIPED, sets);
      swiperDoc.put(MATCH_LIST, new ArrayList<>());
      matchesCollection.insertOne(swiperDoc);
    } else if(RIGHT.equals(message.getSwipeDirection())) {
      Set<String> sets = new HashSet<>(swiperDoc.getList(RIGHT_SWIPED, String.class));
      sets.add(message.getSwipeeId());
      matchesCollection.updateOne(swiperDoc, set(RIGHT_SWIPED, sets));
    }

    if(RIGHT.equals(message.getSwipeDirection())) {
      Document swipeeDoc = matchesCollection.find(eq(USER_ID, message.getSwipeeId())).first();
      if(swipeeDoc != null) {
        List<String> rightSwipeList = swipeeDoc.getList(RIGHT_SWIPED, String.class);
        if(rightSwipeList != null) {
          boolean containsSwiper = rightSwipeList.contains(message.getSwiperId());
          if(containsSwiper) {
            Set<String> matchedSets = new HashSet<>(swiperDoc.getList(MATCH_LIST, String.class));
            matchedSets.add(message.getSwipeeId());
            matchesCollection.updateOne(eq(USER_ID, message.getSwiperId()), set(MATCH_LIST, matchedSets));

            Set<String> matchedSwipeeSets = new HashSet<>(swipeeDoc.getList(MATCH_LIST, String.class));
            matchedSwipeeSets.add(message.getSwiperId());
            matchesCollection.updateOne(eq(USER_ID, message.getSwipeeId()), set(MATCH_LIST, matchedSwipeeSets));
          }
        }
      }
    }
  }
}
