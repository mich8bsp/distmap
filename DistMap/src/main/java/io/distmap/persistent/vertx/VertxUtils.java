package io.distmap.persistent.vertx;

import io.vertx.core.Vertx;

/**
 * Created by mich8bsp on 19-Mar-16.
 */
public class VertxUtils {

    private static Vertx vertx = Vertx.vertx();

    public static Vertx getInstance(){
        return vertx;
    }
}
