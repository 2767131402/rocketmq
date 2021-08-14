package com.zhenglei.rocketmq.p2p;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class Producer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("group");
        producer.setNamesrvAddr("81.70.175.128:9876");
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.start();
        for (int i = 0; i < 5; i++) {
            String property = "stu123";
            String msg = "Hello world" + i;
            Message message = new Message("zhenglei",
                    "TagA",
                    property,
                    msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
            message.putUserProperty("mqttSecondTopic",property);
            producer.send(message);
        }
        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
