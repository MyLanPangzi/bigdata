package com.hiscat.flink.statefun;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

/**
 * @author hiscat
 */
public class GreetFunction implements StatefulFunction {
    @Override
    public void invoke(Context context, Object input) {
        GreetRequest request = (GreetRequest) input;
        GreetResponse res = GreetResponse.builder()
                .who(request.getWho())
                .greeting("hello " + request.getWho())
                .build();
//        context.send();
    }
}
