package main;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import static com.mongodb.client.model.Filters.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;




public class B_Consultas {
    public static void main (String[] args) throws InterruptedException {

        System.out.print("Connecting to COVID_Tweets database...\n");
        System.out.print("Enter the name of the collection that you want to analyze:\t"); //Word that we want to look for in recent tweets
        Scanner input = new Scanner(System.in);
        String collection = input.nextLine();

        //Crear un objeto cliente de mongo
        MongoClient mongo = new MongoClient("127.0.0.1", 27017);
        //Se obtiene la base de datos con la que se va a trabajar. E
        MongoDatabase db1 = mongo.getDatabase("COVID_Tweets");
        //Se obtiene la colleccion de documentos a eliminar
        MongoCollection<Document> coll = db1.getCollection(collection);
        Thread.sleep(3 * 1000);


        System.out.print("\n- Number of Tweets in the collection:\n");
        System.out.println(coll.countDocuments());
        System.out.println("\n");


        System.out.print("\n- Number of Tweets in the collection grouped by language:\n");
        //Se crea una lista donde ir almacenando los diferentes pasos del proceso
        List<Bson> query = new ArrayList<Bson>();
        query.add(Aggregates.group("$language", Accumulators.sum("count",1))); //Se calcular la suma de "tweets por lenguaje
        query.add(Aggregates.sort(Sorts.descending("count")));
        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado que se print por pantalla
        MongoCursor<Document> cursor = coll.aggregate(query).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        System.out.println("\n");



        System.out.print("\n- Number of Tweets in the collection grouped by country:\n");
        List<Bson> query2 = new ArrayList<Bson>();
        query2.add(Aggregates.group("$country", Accumulators.sum("count",1))); //Se calcular la suma de "tweets por country
        query2.add(Aggregates.sort(Sorts.descending("count")));
        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado que se print por pantalla
        cursor = coll.aggregate(query2).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        System.out.println("\n");


        System.out.print("\n- Number of Tweets in the collection per language grouped by country:\n");
        List<Bson> query8 = new ArrayList<Bson>();
        query8.add(Aggregates.match(ne("country", "null")));
        Document groupby = new Document("group", Arrays.asList( new Document("language", "$language"), new Document("country", "$country")));
        query8.add(Aggregates.group(groupby, Accumulators.sum("count",1)));
        query8.add(Aggregates.sort(Sorts.descending("count")));
        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado que se print por pantalla
        cursor = coll.aggregate(query8).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        System.out.println("\n");


        System.out.print("\n- Number of Tweets in the collection per day:\n");
        List<Bson> query9 = new ArrayList<Bson>();
        query9.add(Aggregates.group("$date", Accumulators.sum("count",1))); //Se calcular la suma de "tweets por country
        query9.add(Aggregates.sort(Sorts.descending("count")));
        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado que se print por pantalla
        cursor = coll.aggregate(query9).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }


        System.out.print("\n- Number of Tweets in the collection per day grouped by country:\n");
        List<Bson> query3 = new ArrayList<Bson>();
        groupby = new Document("group", Arrays.asList( new Document ("date", "$date"), new Document("country", "$country")));
        query3.add(Aggregates.group(groupby, Accumulators.sum("count",1)));
        query3.add(Aggregates.sort(Sorts.descending("count")));
        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado que se print por pantalla
        cursor = coll.aggregate(query3).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        System.out.println("\n");



        System.out.print("\n- Number of Tweets in the collection grouped by source:\n");
        List<Bson> query4 = new ArrayList<Bson>();
        query4.add(Aggregates.group("$source", Accumulators.sum("count",1)));
        query4.add(Aggregates.match(gte("count",100)));
        query4.add(Aggregates.sort(Sorts.descending("count")));
        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado que se print por pantalla
        cursor = coll.aggregate(query4).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        System.out.println("\n");


        System.out.print("\n- Number of Tweets in the collection grouped by language and source:\n");
        List<Bson> query5 = new ArrayList<Bson>();
        groupby = new Document("group", Arrays.asList( new Document("language", "$language"), new Document ("source", "$source")));
        query5.add(Aggregates.group(groupby, Accumulators.sum("count",1)));
        query5.add(Aggregates.match(gte("count",50)));
        query5.add(Aggregates.sort(Sorts.descending("count")));
        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado que se print por pantalla
        cursor = coll.aggregate(query5).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        System.out.println("\n");



        System.out.print("\n- Top-10 prolific users:\n");
        List<Bson> query6 = new ArrayList<Bson>();
        query6.add(Aggregates.group("$user.user_screen_name", Accumulators.sum("count",1)));
        query6.add(Aggregates.sort(Sorts.descending("count")));
        query6.add(Aggregates.limit(10));
        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado que se print por pantalla
        cursor = coll.aggregate(query6).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        System.out.println("\n");


        System.out.print("\n- Tweets by most prolific user\n");
        List<Bson> query7 = new ArrayList<Bson>();
        query7.add(Aggregates.group("$user.user_screen_name", Accumulators.sum("count",1),
                Accumulators.push("Tweet", "$tweet.tweet_text")));
        query7.add(Aggregates.sort(Sorts.descending("count")));
        query7.add(Aggregates.limit(1));
        query7.add(Aggregates.project(Projections.fields(Projections.include("Tweet"))));
        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado que se print por pantalla
        cursor = coll.aggregate(query7).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        System.out.println("\n");



        //Cerrar la conexión con la base de datos
        mongo.close();

    }

}
