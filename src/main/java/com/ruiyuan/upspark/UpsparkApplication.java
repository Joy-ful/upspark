package com.ruiyuan.upspark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class UpsparkApplication {

    public static void main(String[] args) {
        SpringApplication.run(UpsparkApplication.class, args);
    }
    @RestController
    public class IndexController {
        @RequestMapping("/")
        public String index() {
            return "hello, world";
        }
    }
}
