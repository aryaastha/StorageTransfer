package Process;

import DataSource.Datasources;
import Factory.SinkFactory;
import Factory.SourceFactory;
import Service.ProcessService;
import Utils.Constants;
import Utils.FileUtils;
import Utils.GsonService;
import Utils.JobProperties;
import com.google.gson.JsonObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by astha.a on 17/05/17.
 */
public class MoveNewProcess {
    private Datasources sourceObject;
    ProcessService processService;
    Datasources sinkObject;

    public MoveNewProcess(JavaSparkContext jsc) throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        sourceObject = SourceFactory.getSourceObject();
        JsonObject preProcessJson = GsonService.getInstance().getGson().fromJson(FileUtils.readFileToString(JobProperties.getInstance().getProperty(Constants.PREPROCESS_PROPERTIES)), JsonObject.class);
        processService = new ProcessService(jsc, preProcessJson);
        sinkObject = SinkFactory.getSinkObject();
    }

    public void process(JavaSparkContext jsc) throws IOException {
        JavaRDD<JsonObject> jsonJavaRDD = sourceObject.get(jsc);
        JavaRDD<JsonObject> afterPreprocessing = processService.process(jsonJavaRDD);
        sinkObject.put(afterPreprocessing, jsc);
    }
}
