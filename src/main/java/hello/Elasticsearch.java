package hello;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

public class Elasticsearch {
    public static void main(String[] args) throws Exception {
        getJson();
    }

    public static void getJson() throws Exception {
        // String url = "http://192.168.67.240:32614/" +
        // "network_traffic_ela/" +
        // "_search?" +
        // "q=time_epoch:[1577263234.367915457+TO+1577263235.367915457]&" +
        // "sort=time_epoch.keyword:asc&" +
        // "size=1";

        String start_time = getStartTime();
        String end_time = String.format("%.9f", new Double(start_time) + 10);
        System.out.println("start_time: " + start_time);
        System.out.println("end_time: " + end_time);

        String url = "http://192.168.67.240:32614/" + "network_traffic_ela/" + "_search?" + "q=time_epoch:["
                + start_time + "+TO+" + end_time + "]";
        System.out.println("url: " + url);
        String pattern = "\"time_epoch\":\"(\\d{10}\\.\\d{9})\",";
        String json = IOUtils.toString(new URL(url).openConnection().getInputStream());
        getStringfromJson(pattern, json);
        // System.out.println("time_epoch: " + getStringfromJson(pattern, json));
        // System.out.println(json);
    }

    public static String getStartTime() {
        String start_time_url = "http://192.168.67.240:32614/network_traffic_ela/_search?_source_includes=time_epoch&sort=time_epoch.keyword:asc&size=1";
        String start_time_pattern = "\"time_epoch\":\"(\\d{10}\\.\\d{9})\",";
        try {
            Matcher matcher = Pattern.compile(start_time_pattern)
                    .matcher(IOUtils.toString(new URL(start_time_url).openConnection().getInputStream()));
            if (matcher.find()) {
                return matcher.group(1);
            } else {
                System.out.println("no match");
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void getStringfromJson(String pattern, String json) {
        Matcher matcher = Pattern.compile(pattern).matcher(json);
        while (matcher.find()) {
            System.out.println(matcher.group(1));
        }
    }
}