import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import com.mongodb.connection.ClusterSettings;
import org.bson.Document;

import java.util.Arrays;

public class MongoConfig {
    /*
        private MongoClient primaryMongoClient;
        private MongoDatabase primaryDatabase;
        private MongoClient secondaryMongoClient;
        private MongoDatabase secondaryDatabase;
        private static String primaryDB = "TwinderDB";
        private static String collectionName = "twinders";
        private static String secondaryDB = "TwinderSecondary";

     */
    private static MongoConfig instance = null;
    private MongoClient primaryMongoClient;
    private MongoClient secondaryMongoClient;
    private MongoDatabase primaryDatabase;
    private MongoDatabase secondaryDatabase;
    private static String primaryDatabaseName = "TwinderDB";
    private static String secondaryDatabaseName = "TwinderSecondary";
    private static String collectionName = "twinders";
    private static String primaryEC2IP = "35.91.171.7";
    private static String secondaryEC2IP = "54.244.131.7";
    private static String mongoDB_id = "admin";
    private static String mongoDB_pw = "password";

    //String ec2IPv4ForMongoDB = "54.191.180.231";


    public static MongoConfig getInstance() {
        if(instance == null)
            return new MongoConfig();
        return instance;
    }

    public MongoConfig() {

        // works well without read preference
//        String primaryConnectionString = "mongodb://admin:password@" + primaryEC2IP + ":27017/?maxPoolSize=50";
//        primaryMongoClient = MongoClients.create(primaryConnectionString);
//        primaryDatabase = primaryMongoClient.getDatabase(primaryDatabaseName);



        /**
         * 1. try this. When using string to create MongoClient, queue delivery rate is very fast. But when using settings, queue delivery rate is very slow.
         * 2. The speed of writing into DB is slow so that all get requests give error.
         * 3. Mongo Compass not working somehow
         * 4. try to run consumer in AWS instance
         * 5. try everything with get servlet
         */


//        ConnectionString connectionString = new ConnectionString("mongodb://admin:password@" + primaryEC2IP + ":27017/?maxPoolSize=50&replicaSet=rs1");
        ConnectionString connectionString = new ConnectionString("mongodb://admin:password@" + primaryEC2IP + ":27017/?maxPoolSize=50");
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .readPreference(ReadPreference.secondaryPreferred()) // set the read preference to secondary preferred
                .build();

        primaryMongoClient = MongoClients.create(settings);
        primaryDatabase = primaryMongoClient.getDatabase(primaryDatabaseName);


    }

    public MongoClient getMongoClient() {
        return primaryMongoClient;
    }

    public MongoDatabase getDatabase() {
        return primaryDatabase;
    }
}
