import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class Writer implements Runnable {

    //private static String SWIPE_DB = "swipe";
    private static String COLLECTION_NAME = "twinders";
    private BlockingQueue<List<WriteModel<Document>>> queue;
    private static MongoCollection<Document> swipeCollection;

    public Writer(BlockingQueue<List<WriteModel<Document>>> queue) {
        this.queue = queue;

        MongoConfig mongoConfig = MongoConfig.getInstance();
        MongoDatabase primaryDB = mongoConfig.getDatabase();

        swipeCollection = primaryDB.getCollection(COLLECTION_NAME);
    }


    @Override
    public void run() {
        while(true) {
            try {
                List<WriteModel<Document>> item = queue.take();
                BulkWriteOptions options = new BulkWriteOptions().ordered(false);
                swipeCollection.bulkWrite(item, options);
                //swipeCollection.bulkWrite(item, new BulkWriteOptions().ordered(false));

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
