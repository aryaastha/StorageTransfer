package Process;

import com.google.gson.JsonObject;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by astha.a on 17/05/17.
 */
public interface Processes {
    public JavaRDD<JsonObject> apply(JavaRDD<JsonObject> input);
}
