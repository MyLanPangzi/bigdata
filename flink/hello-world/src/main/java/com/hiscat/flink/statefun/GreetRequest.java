package com.hiscat.flink.statefun;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author hicat
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class GreetRequest {
    private String who;
}
