/*
 * Copyright (C) 2016 Media.net Advertising FZ-LLC All Rights Reserved
 */

package Utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Created by aitch_nu on 25/11/15.
 */
public class GsonService {

    private static GsonService instance;

    private Gson gson;

    public static GsonService getInstance() {
        if (instance == null) {
            instance = new GsonService();
        }
        return instance;
    }

    private GsonService() {
        gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().serializeNulls().create();
    }

    public Gson getGson() {
        return gson;
    }


}
