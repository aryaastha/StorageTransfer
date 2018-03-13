package DataSource;

import com.google.gson.JsonObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by astha.a on 07/12/17.
 */
public interface Datasources {
    public JavaRDD<JsonObject> get(JavaSparkContext sparkContext);

    public void put(JavaRDD<JsonObject> dataForSink, JavaSparkContext context);
}
