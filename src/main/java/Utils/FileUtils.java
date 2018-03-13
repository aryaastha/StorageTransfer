/*
 * Copyright (C) 2016 Media.net Advertising FZ-LLC All Rights Reserved
 */

package Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by aitch_nu on 28/10/15.
 */
public class FileUtils {

    public static List<String> getRuleStrings(String files) {
        List<String> ruleStrings = new ArrayList<String>();
        for (String file : files.split(",")) {
            ruleStrings.add(FileUtils.readFileToString(file.trim()));
        }
        return ruleStrings;
    }

    public static void updatePropertiesFromHdfsFile(JobProperties jobProperties, String path) {
        String getPropertyString = readFileToString(path);
        for (String line : getPropertyString.split("\n")) {
            String key = line.split("=")[0].trim();
            String value = line.split("=")[1].trim();
            jobProperties.setProperty(key, value);
        }
    }

    public static void updatePropertiesFromHdfsFile(Properties jobProperties, String path) {
        //testMe
        String getPropertyString = readFileToString(path);
        for (String line : getPropertyString.split("\n")) {
            String key = line.split("=")[0].trim();
            String value = line.split("=")[1].trim();
            jobProperties.setProperty(key, value);
        }
    }

    public static String readFileToString(String filePath) {
        String result = "";
        try {
            Path pt = new Path(filePath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

            String line = br.readLine();
            while (line != null) {
                result = result + line + "\n";
                line = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void writeFileToHdfs(Path file, StringBuilder stringBuffer) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        OutputStream out = null;
        if (fs.exists(file)) {
            out = fs.append(file);
        } else {
            out = fs.create(file);
        }

        InputStream in = new ByteArrayInputStream(stringBuffer.toString().getBytes());

        IOUtils.copyBytes(in, out, conf);

        in.close();
        out.close();
    }

    public static boolean isSuccessFlagPresent(String path) {
        Path dirPath = new Path(path + "_SUCCESS");
        try {
            final FileSystem fs = FileSystem.get(new Configuration());
            if (fs.exists(dirPath)) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void deleteDirIfExists(String path) {
        Path dirPath = new Path(path);
        try {
            final FileSystem fs = FileSystem.get(new Configuration());
            if (fs.exists(dirPath)) {
                FileStatus[] fileStatuses = fs.listStatus(dirPath, new PathFilter() {
                    public boolean accept(Path path) {
                        try {
                            return (!path.getName().startsWith("_")) && fs.isDirectory(path);
                        } catch (IOException e) {
                            e.printStackTrace();
                            return true;
                        }
                    }
                });
                if (fileStatuses.length == 0) {
                    fs.delete(dirPath, true);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
