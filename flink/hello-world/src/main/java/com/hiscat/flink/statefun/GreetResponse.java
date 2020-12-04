package com.hiscat.flink.statefun;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author hiscat
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class GreetResponse {
    private String who;
    private String greeting;

    public String getWho() {
        return who;
    }

    public void setWho(String who) {
        this.who = who;
    }

    public String getGreeting() {
        return greeting;
    }

    public void setGreeting(String greeting) {
        this.greeting = greeting;
    }
}
