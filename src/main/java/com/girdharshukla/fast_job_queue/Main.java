package com.girdharshukla.fast_job_queue;


import io.helidon.logging.common.LogConfig;
import io.helidon.config.Config;
import io.helidon.service.registry.Services;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;


public class Main {


    private Main() {
    }

    public static void main(String[] args) {
        
        LogConfig.configureRuntime();

        Config config = Config.create();
        Services.set(Config.class, config);


        WebServer server = WebServer.builder()
                .config(config.get("server"))
                .routing(Main::routing)
                .build()
                .start();


        System.out.println("WEB server is up! http://localhost:" + server.port() + "/simple-greet");

    }

    static void routing(HttpRouting.Builder routing) {
      
    }
}