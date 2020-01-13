package hello;

import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.EsSpark;
import scala.Tuple2;
import scala.collection.Map;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.LinkedHashMap;
//import java.util.Map; 
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Hello {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello World");
        // System.out.println(System.getProperty("user.dir"));
        // hopCount();
        //writeJson();
        getJson2();
    }

    public static void hopCount() throws Exception {
        org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF);
        SparkSession ss = SparkSession.builder().appName("HopCount").master("local")
                .config("es.index.auto.create", "true").config("es.nodes", "192.168.67.240").config("es.port", "32614")
                .config("es.nodes.wan.only", "true")
                // .config("es.scroll.limit", 1)
                .getOrCreate();

        SparkContext sc = ss.sparkContext();
        StringBuffer sb = new StringBuffer();
        readToBuffer(sb);
        String esQuery = sb.toString();

        RDD<Tuple2<String, Map<String, Object>>> resRdd = EsSpark.esRDD(sc, "network_traffic_ela", esQuery);
        System.out.println("resRdd.count(): " + resRdd.count());
        @SuppressWarnings("unchecked")
        Tuple2<String, Map<String, Object>>[] tuples = (Tuple2<String, Map<String, Object>>[]) resRdd.collect();
        // int count = (int) resRdd.count();
        // System.out.println(tuples[count - 1]);
        // System.out.println(tuples[0]);
        for (int i = 0; i < resRdd.count(); i++) {
            // Tuple2<String,Map<String, Object>> tuple = tuples[i];
            System.out.println(tuples[i]);
        }

        ss.stop();

    }

    public static void readToBuffer(StringBuffer buffer) throws IOException {
        InputStream is = new FileInputStream("src/main/java/hello/test1.json");
        String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        line = reader.readLine();
        while (line != null) {
            buffer.append(line);
            buffer.append("\n");
            line = reader.readLine();
        }
        // System.out.println(buffer);
        reader.close();
        is.close();
    }

    public static void writeJson() throws FileNotFoundException {
        // creating JSONObject
        JSONObject jo = new JSONObject();

        // putting data to JSONObject
        jo.put("firstName", "John");
        jo.put("lastName", "Smith");
        jo.put("age", 25);

        // for address data, first create LinkedHashMap
        java.util.Map m = new LinkedHashMap(4);
        m.put("streetAddress", "21 2nd Street");
        m.put("city", "New York");
        m.put("state", "NY");
        m.put("postalCode", 10021);

        // putting address to JSONObject
        jo.put("address", m);

        // for phone numbers, first create JSONArray
        JSONArray ja = new JSONArray();

        m = new LinkedHashMap(2);
        m.put("type", "home");
        m.put("number", "212 555-1234");

        // adding map to list
        ja.add(m);

        m = new LinkedHashMap(2);
        m.put("type", "fax");
        m.put("number", "212 555-1234");

        // adding map to list
        ja.add(m);

        // putting phoneNumbers to JSONObject
        jo.put("phoneNumbers", ja);

        // writing JSON to file:"JSONExample.json" in cwd
        PrintWriter pw = new PrintWriter("JSONExample.json");
        pw.write(jo.toJSONString());
        System.out.println(pw.toString());

        pw.flush();
        pw.close();
    }

    public static void getJson() throws FileNotFoundException, IOException, ParseException {
        Object obj = new JSONParser().parse(new FileReader("JSONExample.json"));
        JSONObject jo = (JSONObject) obj;

        // getting firstName and lastName
        String firstName = (String) jo.get("firstName");
        String lastName = (String) jo.get("lastName");

        System.out.println(firstName);
        System.out.println(lastName);

        // getting age
        //long age = (long) jo.get("age");
        Long age = (Long) jo.get("age");
        System.out.println(age);

        // getting address
        java.util.Map address = ((java.util.Map) jo.get("address"));

        // iterating address Map
        Iterator<java.util.Map.Entry> itr1 = address.entrySet().iterator();
        while (itr1.hasNext()) {
            java.util.Map.Entry pair = itr1.next();
            System.out.println(pair.getKey() + " : " + pair.getValue());
        }

        // getting phoneNumbers
        JSONArray ja = (JSONArray) jo.get("phoneNumbers");

        // iterating phoneNumbers
        Iterator itr2 = ja.iterator();

        while (itr2.hasNext()) {
            itr1 = ((java.util.Map) itr2.next()).entrySet().iterator();
            while (itr1.hasNext()) {
                java.util.Map.Entry pair = itr1.next();
                System.out.println(pair.getKey() + " : " + pair.getValue());
            }
        }
    }

    public static void getJson2() throws FileNotFoundException, IOException, ParseException {
        Object obj = new JSONParser().parse(new FileReader("src/main/java/hello/test1.json"));
        JSONObject jo = (JSONObject) obj;
        System.out.println(jo);
    }

}
