package main;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Scanner;

public class C_DeleteCollection {
    public static void main (String[] args){

        System.out.print("Connecting to COVID_Tweets database...\n");
        System.out.print("Enter the name of the collection that you want to delete:\t"); //Word that we want to look for in recent tweets
        Scanner input = new Scanner(System.in);
        String collection = input.nextLine();

        //Crear un objeto cliente de mongo
        MongoClient mongo = new MongoClient("127.0.0.1", 27017);

        //Se obtiene la base de datos con la que se va a trabajar. E
        MongoDatabase db1 = mongo.getDatabase("COVID_Tweets");

        //Se obtiene la colleccion de documentos a eliminar
        MongoCollection<Document> coll = db1.getCollection(collection);

        coll.drop();


        //Cerrar la conexión con la base de datos
        mongo.close();

    }
}
