package main;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.util.Scanner;

public class D_DeleteDatabase {
    public static void main (String[] args) {

        System.out.print("Are you sure you want to delete COVID_Tweets database? (y/n)\t");
        Scanner input = new Scanner(System.in);
        String response = input.nextLine();

        //Crear un objeto cliente de mongo
        MongoClient mongo = new MongoClient("127.0.0.1", 27017);

        if(response.equals("y")) {
            //Se obtiene la base de datos con la que se va a eliminar
            MongoDatabase db1 = mongo.getDatabase("COVID_Tweets");
            db1.drop();
            System.out.print("\nCOVID_Tweets deleted. Bye\n");
            mongo.close();
        } else if (response.equals("n")){
            System.out.print("\nBye\n");
            mongo.close();
        }

    }
}
