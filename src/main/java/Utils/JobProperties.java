/*
 * Copyright (C) 2016 Media.net Advertising FZ-LLC All Rights Reserved
 */

package Utils;

import java.util.Map;
import java.util.Properties;

/**
 * Created by aitch_nu on 4/1/16.
 */
public class JobProperties {

    private static JobProperties instance = null;

    private Properties properties;

    public static JobProperties getInstance() {
        if (instance == null) {
            instance = new JobProperties();
        }
        return instance;
    }

    private JobProperties() {
        properties = new Properties();
    }

    void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    public String getProperty(String key) {
        System.out.println("Property requested : " + key);
        return properties.getProperty(key);
    }

    void printProperties() {
        for (Map.Entry<Object, Object> e : properties.entrySet()) {
            System.out.println(e);
        }
    }
}
