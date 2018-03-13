package Service;

import Process.Processes;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by astha.a on 17/05/17.
 */
public class ProcessService implements Serializable {
    Broadcast<List<Processes>> processes;

    public ProcessService(JavaSparkContext jsc, JsonObject jsonObject) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        List<Processes> tempProcesses = new ArrayList<Processes>();
        Set<Map.Entry<String, JsonElement>> entries = jsonObject.entrySet();
        for (Map.Entry<String, JsonElement> e : entries) {
            Class ProcessName = Class.forName("Process." + e.getKey());
            Constructor declaredConstructor;
            System.out.println("Value of properties json : " + e.getValue().isJsonArray());
            if (e.getValue().isJsonArray()) {
                declaredConstructor = ProcessName.getDeclaredConstructor(JsonArray.class);
                tempProcesses.add((Processes) declaredConstructor.newInstance(e.getValue().getAsJsonArray()));
            } else {
                declaredConstructor = ProcessName.getDeclaredConstructor(JsonObject.class);
                tempProcesses.add((Processes) declaredConstructor.newInstance(e.getValue().getAsJsonObject()));
            }
        }
        this.processes = jsc.broadcast(tempProcesses);
    }

    public JavaRDD<JsonObject> process(final JavaRDD<JsonObject> input) {
        ArrayList<JavaRDD<JsonObject>> arrayList = new ArrayList<JavaRDD<JsonObject>>();
        arrayList.add(input);
        System.out.println("Number of Processes : " + processes.getValue().size());
        for (Processes process : processes.getValue()) {
            JavaRDD<JsonObject> jsonObjectJavaRDD = arrayList.get(0);
            arrayList.remove(0);
            JavaRDD<JsonObject> apply = process.apply(jsonObjectJavaRDD);
            arrayList.add(apply);
        }
        return arrayList.get(0);
    }
}
