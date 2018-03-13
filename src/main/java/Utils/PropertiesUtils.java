/*
 * Copyright (C) 2016 Media.net Advertising FZ-LLC All Rights Reserved
 */

package Utils;

import org.apache.commons.cli.*;

/**
 * Created by aitch_nu on 4/2/16.
 */
public class PropertiesUtils {

    public static void loadJobProperties(String args[]) throws ParseException {
        JobProperties jobProperties = JobProperties.getInstance();
        Options options = new Options();
        options.addOption("f", true, "properties file");
        options.addOption(Constants.DATA_PARTITION.replace('.', '_'), true, "input dir partition");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        FileUtils.updatePropertiesFromHdfsFile(jobProperties, cmd.getOptionValue("f"));
        try {
            jobProperties.setProperty(Constants.DATA_PARTITION, cmd.getOptionValue(Constants.DATA_PARTITION.replace('.', '_')));
            jobProperties.setProperty(Constants.TIMESTAMP, cmd.getOptionValue(Constants.DATA_PARTITION.replace('.', '_')).split("=")[1]);
        } catch (NullPointerException e) {
            System.out.println("Skipping data partition, missing from argument list");
        }
        jobProperties.printProperties();
    }
}
