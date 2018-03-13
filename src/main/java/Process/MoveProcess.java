package Process;

import DataSource.Datasources;
import Factory.SourceFactory;
import Service.HbaseService;
import Service.PreprocessService;
import com.google.gson.JsonObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;

/**
 * Created by astha.a on 11/05/17.
 */
public class MoveProcess {
    Datasources sourceObject;
    PreprocessService preprocessService;
    HbaseService hbaseService;

    public MoveProcess() throws ClassNotFoundException {
        sourceObject = SourceFactory.getSourceObject();
//        JsonObject preProcessJson = GsonService.getInstance().getGson().fromJson(FileUtils.readFileToString(JobProperties.getInstance().getProperty(Conf.PREPROCESS_PROPERTIES)),JsonObject.class);
//        preprocessService = new PreprocessService(preProcessJson);
//        hbaseService = GsonService.getInstance().getGson().fromJson(FileUtils.readFileToString(JobProperties.getInstance().getProperty(Conf.HBASE_PROPERTIES)), HbaseService.class);
    }

    public void process(JavaSparkContext jsc) throws IOException {
        JavaRDD<JsonObject> jsonJavaRDD = sourceObject.get(jsc);
//        JavaRDD<JsonObject> apply = preprocessService.apply(jsonJavaRDD);
//        hbaseService.putAllInHbase(apply);

        List<JsonObject> take = jsonJavaRDD.take(1);
        for (JsonObject json : take) {
            System.out.println(json);
        }

    }
}
