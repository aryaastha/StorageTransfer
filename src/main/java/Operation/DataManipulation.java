package Operation;

import Utils.Utils;
import com.google.gson.JsonObject;

import java.util.List;

public class DataManipulation {
    public static String generateNewValue(JsonObject json, List<String> fields) {
//        System.out.println("I came here!");
        StringBuilder builder = new StringBuilder();
        for (String field : fields) {
            builder.append(json.get(field).getAsString()).append(";");
        }
        return builder.toString();
    }

    public static String cleanText(JsonObject json, List<String> fields) {
        StringBuilder builder = new StringBuilder();
        for (String field : fields) {
            builder.append(json.get(field).getAsString());
        }
        String cleanFields = Utils.cleanText(builder.toString());
        return cleanFields;
    }

    public static String cleanAdUrl(JsonObject json, List<String> fields) {
        StringBuilder builder = new StringBuilder();
        for (String field : fields) {
            builder.append(json.get(field).getAsString());
        }
        String cleanFields = Utils.cleanAdUrl(builder.toString());
        return cleanFields;
    }

    public static String makeHash(JsonObject json, List<String> fields) {
        StringBuilder builder = new StringBuilder();
        for (String field : fields) {
            builder.append(json.get(field).getAsString());
        }
        String cleanFields = Utils.getMD5Hex(builder.toString());
        return cleanFields;
    }

    public static String adDumpSource(JsonObject json, List<String> fields) {
        return "ad_details";
    }
}