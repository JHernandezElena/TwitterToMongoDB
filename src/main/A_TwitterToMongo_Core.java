package main;

import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.IndexOptions;
import static com.mongodb.client.model.Projections.*;

import java.util.Scanner;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;





public class A_TwitterToMongo_Core {


    private ConfigurationBuilder cb;
    private MongoDatabase db;
    private MongoCollection<Document> items;




    /**
     * static block used to construct a connection with tweeter with twitter4j
     * configuration with provided settings. This configuration builder will be
     * used for next search action to fetch the tweets from twitter.com.
     */

    public static void main(String[] args) throws InterruptedException {

        A_TwitterToMongo_Core taskObj = new A_TwitterToMongo_Core();
        taskObj.loadMenu();
    }


    //FUNCTION TO LOAD THE TWEETS
    public void loadMenu() throws InterruptedException {

        System.out.print("Connecting to COVID_Tweets database...\n");

        System.out.print("Please choose your Keyword that the tweet must contain (it will be the name of your collection):\t"); //Word that we want to look for in recent tweets
        Scanner input = new Scanner(System.in);
        String keyword = input.nextLine();

        connectdb(keyword); //Calls public void connectdb function bellow

        while(true)
        {
            cb = new ConfigurationBuilder()
                    .setDebugEnabled(true)
                    .setOAuthConsumerKey("xxx") //INPUT YOUR KEYS HERE
                    .setOAuthConsumerSecret("xxx")
                    .setOAuthAccessToken("xxx")
                    .setOAuthAccessTokenSecret("xxx")
                    .setTweetModeExtended(true); //set this to get the full tweet text (if false, tweet will be truncated)

            getTweetByQuery(true,keyword);  //Calls get tweet by query function bellow
            cb = null;

            Thread.sleep(60 * 1000);              // wait 1 minute
        }

    }

    //FUNCTION THAT CREATES CONECTION TO MONGODB
    public void connectdb(String keyword) {
        try {
            System.out.println("Connecting to Mongo DB..");
            MongoClient mongo = new MongoClient("127.0.0.1", 27017);

            //GET DATABASE (it creates it if doesnt exist)
            db = mongo.getDatabase("COVID_Tweets");
            //GET/CREATE COLLECTION BASED IN KEYWORD INUTTED
            items = db.getCollection(keyword);

            //make the tweet_ID unique in the database
            IndexOptions indexOptions = new IndexOptions().unique(true);
            items.createIndex(Indexes.ascending("tweet_ID"), indexOptions);

        } catch (MongoException ex) {
            System.out.println("MongoException :" + ex.getMessage());
        }

    }



    //FUNCTION TO EXTRACT DESIRED INFO OF TWEETS AND INSERT IT INTO MONGODB (Document=tweet with desired inf)
    public void getTweetByQuery(boolean loadRecords, String keyword) throws InterruptedException {

        //ConNects to twitter
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        if (cb != null) {

            try {
                Query query = new Query(keyword); //Finds tweet by the previously inputted keyword
                query.setCount(100); //includes 100 tweets at a time (set to not overload Mongodb docker)
                QueryResult result;
                result = twitter.search(query);
                System.out.println("Getting Tweets...");
                List<Status> tweets = result.getTweets();

                for (Status tweet : tweets) {

                    //To be able to get info about the mentions of the tweet
                    UserMentionEntity[] mentioned = tweet.getUserMentionEntities();

                    //Create a timestamp readable pattern
                    String pattern = "MM/dd/yyyy HH:mm:ss";
                    DateFormat df = new SimpleDateFormat(pattern);
                    String DateAsString = df.format(tweet.getCreatedAt());
                    String DayAsString = DateAsString.split(" ")[0];
                    String BirthAsString = df.format(tweet.getUser().getCreatedAt());


                    //Get country if geolocation of tweet is activated
                    Place place = tweet.getPlace();
                    String country;
                    if (place != null) {
                        country = place.getCountry();
                    } else {
                        country = "null";
                    }

                    //Extract the info we want from each tweeet
                    Document doc = new Document ("tweet_ID", tweet.getId())
                            .append("user", new Document("user_screen_name", tweet.getUser().getScreenName())
                                    .append("user_name", tweet.getUser().getName())
                                    .append("user_followers", tweet.getUser().getFollowersCount())
                                    .append("user_location", tweet.getUser().getLocation())
                                    .append("user_creation", BirthAsString)
                                    .append("user_followers", tweet.getUser().getFollowersCount()))
                            .append("tweet", new Document("tweet_text", tweet.getText())
                                    .append("is_retweet", tweet.isRetweet())
                                    .append("retweet_count", tweet.getRetweetCount())
                                    .append("favorited", tweet.isFavorited())
                                    .append("tweet_mentions_count", mentioned.length))
                            .append("source",tweet.getSource())
                            .append("timestamp",DateAsString)
                            .append("date",DayAsString)
                            .append("language", tweet.getLang())
                            .append("country", country);



                    //Insert document into MongoDB
                    try {
                        items.insertOne(doc);
                    } catch (Exception e) {
                        System.out.println("MongoDB Connection Error : " + e.getMessage());

                    }
                }

                // Printing fetched records from DB to console.
                if (loadRecords) {
                    getTweetsRecords(); //Calls getTweetRecords function bellow
                    //System.out.println(items.countDocuments()); //count documents in collection
                }

            } catch (TwitterException te) {
                System.out.println("te.getErrorCode() " + te.getErrorCode());
                System.out.println("te.getExceptionCode() " + te.getExceptionCode());
                System.out.println("te.getStatusCode() " + te.getStatusCode());
                if (te.getStatusCode() == 401) {
                    System.out.println("Twitter Error : \nAuthentication credentials (https://dev.twitter.com/pages/auth) were missing or incorrect.\nEnsure that you have set valid consumer key/secret, access token/secret, and the system clock is in sync.");
                } else {
                    System.out.println("Twitter Error : " + te.getMessage());
                }


            }
        } else {
            System.out.println("MongoDB is not Connected! Please check mongoDB intance running..");
        }
    }


    //FUNCTION TO PRINT THE RECORDS WE ARE INPUTTING INTO MONGO
    public void getTweetsRecords() throws InterruptedException {
        MongoCursor<Document> cursor = items.find()
                .projection(fields(include("timestamp", "user.user_screen_name", "tweet.tweet_text"), excludeId()))
                .iterator();


        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }

    }


}
