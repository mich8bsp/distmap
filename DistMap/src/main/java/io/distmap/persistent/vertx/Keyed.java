package io.distmap.persistent.vertx;

import java.lang.reflect.Field;

/**
 * Created by mich8bsp on 19-Mar-16.
 */
public interface Keyed {

    Field[] getKeyFields();
}
