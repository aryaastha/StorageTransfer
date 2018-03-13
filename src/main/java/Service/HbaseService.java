package Service;

import Utils.HbaseUtil;
import com.google.gson.JsonObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by astha.a on 10/05/17.
 */
public class HbaseService implements Serializable {
    HbaseUtil hbaseUtil;

    public void putAllInHbase(JavaRDD<JsonObject> rdd) throws IOException {
        rdd.foreachPartition(new VoidFunction<Iterator<JsonObject>>() {
            public void call(Iterator<JsonObject> jsonObjectIterator) throws Exception {
                hbaseUtil.putBulk(jsonObjectIterator);
            }
        });
    }


}
