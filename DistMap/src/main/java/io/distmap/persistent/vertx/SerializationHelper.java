package io.distmap.persistent.vertx;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;

import java.io.IOException;

/**
 * Created by mich8bsp on 19-Mar-16.
 */
public class SerializationHelper {

    public static JsonObject saveObjectToJson(Object object) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String jsonAsString = mapper.writeValueAsString(object);
            return new JsonObject(jsonAsString);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static <ReturnedObject> ReturnedObject readObjectFromJson(JsonObject json, Class<? extends ReturnedObject> returnedObjectClass) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json.encode(), returnedObjectClass);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
