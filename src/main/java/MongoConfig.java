import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class MongoConfig {

    private MongoClient mongoClient;
    private MongoDatabase database;
    private static MongoConfig instance = null;

    public static MongoConfig getInstance() {
        if(instance == null)
            return new MongoConfig();
        return instance;
    }

    public MongoConfig() {
        String connectionString = "mongodb+srv://twinder_username:twinder_password@twindercluster.rdueczb.mongodb.net/twinder?retryWrites=true&w=majority";
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .build();
        mongoClient = MongoClients.create(settings);
        database = mongoClient.getDatabase("twinder");
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public MongoDatabase getDatabase() {
        return database;
    }
}
