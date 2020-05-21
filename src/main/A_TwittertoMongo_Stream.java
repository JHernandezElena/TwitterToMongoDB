package main;

import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.IndexOptions;
import static com.mongodb.client.model.Projections.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.text.DateFormat;
import java.text.SimpleDateFormat;



import org.bson.Document;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;


public class A_TwittertoMongo_Stream {

    private ConfigurationBuilder cb;
    private MongoDatabase db;
    private MongoCollection<Document> items;


    /**
     * static block used to construct a connection with tweeter with twitter4j
     * configuration with provided settings. This configuration builder will be
     * used for next search action to fetch the tweets from twitter.com.
     */
    public static void main(String[] args) throws InterruptedException {

        A_TwittertoMongo_Stream stream = new A_TwittertoMongo_Stream();
        stream.loadMenu();

    }


    //FUNCTION TO LOAD THE TWEETS
    public void loadMenu() throws InterruptedException {


        System.out.print("Connecting to COVID_Tweets database...\n");

        System.out.print("Please choose your Collection (will be created if does not exist):\t");
        Scanner input = new Scanner(System.in);
        String collection = input.nextLine();

        System.out.print("Please input list of words that must appear in the tweet (OR list):\t"); //Words that we want to look for in recent tweets
        String words = input.nextLine();
        String [] keywords = words.split("\\s+");

        connectdb(collection); //Calls public void connectdb function bellow


        cb = new ConfigurationBuilder()
                .setDebugEnabled(true)
                .setOAuthConsumerKey("xxx") //INPUT YOUR KEYS HERE
                .setOAuthConsumerSecret("xxx")
                .setOAuthAccessToken("xxx")
                .setOAuthAccessTokenSecret("xxx")
                .setTweetModeExtended(true); //set this to get the full tweet text (if false, tweet will be truncated)

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {

            public void onStatus(Status tweet) {
                // Printing fetched records from DB to console.
                System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());

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
                Document doc = new Document("tweet_ID", tweet.getId())
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
                        .append("source", tweet.getSource())
                        .append("timestamp", DateAsString)
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

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        //Filters the tweets by the list of keywords previously inputted
        FilterQuery fq = new FilterQuery();
        fq.track(keywords);

        twitterStream.addListener(listener);
        twitterStream.filter(fq);

    }


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
}



