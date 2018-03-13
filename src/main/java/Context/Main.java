package Context;


import Process.MoveNewProcess;
import Utils.Constants;
import Utils.JobProperties;
import Utils.PropertiesUtils;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;


/**
 * Created by astha.a on 09/05/17.
 */
public class Main {
    public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        PropertiesUtils.loadJobProperties(args);
        SparkConf conf = new SparkConf().setAppName(JobProperties.getInstance().getProperty(Constants.APP_NAME));
        JavaSparkContext sc = new JavaSparkContext(conf);
        MoveNewProcess moveProcess = new MoveNewProcess(sc);
        moveProcess.process(sc);
    }
}
