package com.test.cluster;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.concurrent.TimeUnit;

/**
 * @Description:
 * @Author: tl
 * @Date: 2019-06-02 10:31
 * @Version: 1.0
 */
public class ProducerListener {

    public static void sendMessage(String datas) {
        // 连接工厂
        ConnectionFactory factory = null;
        // 连接
        Connection connection = null;
        // 目的地
        Destination destination = null;
        // 会话
        Session session = null;
        // 消息发送者
        MessageProducer messageProducer = null;
        // 消息
        Message message = null;

        try {
            // 创建工厂，三个参数分为别：用户名、密码、链接地址
            // 无参构造：默认链接地址为localhost
            // 单参数构造：无验证模式，不需要传递用户名和密码
            // 三参数构造：认证+地址，端口默认61616，在ActiveMQ/conf/activemq.xml中查看
            // failover - 失败转移，当任意节点宕机，自动转移数据并连接选举之后的主节点
            factory = new ActiveMQConnectionFactory("admin", "admin",
                    "failover:(tcp://192.168.76.136:61616,tcp://192.168.76.136:61617,tcp://192.168.76.136:61618)");
            // 创建连接，方法有重载：createConnection(String name, String password)
            // 可以在创建工厂时不指定用户名和密码，在此处指定
            connection = factory.createConnection();
            /*
                开启连接，建议启动连接，消息的发送者会有检测，发送前如果未启动则启动
                消息的消费者必须手动启动连接
             */
            connection.start();
            /*
                创建会话必须的两个参数：是否支持事务、如何确认消息处理
                transacted - boolean：true - 支持，false - 不支持
                    true：支持事务，第二个参数对于Producer默认无效，建议传递Session.SESSION_TRANSACTED
                    false：不支持事务，常用，第二个参数必须传递有效参数，如Session.CLIENT_ACKNOWLEDGE
                acknowledgeMode - int：如何确认消息的处理，使用确认机制实现
                    AUTO_ACKNOWLEDGE：自动确认消息，消费者消费后自动确认，商业开发不推荐
                    CLIENT_ACKNOWLEDGE：客户端手动确认，消费者消费消息后手动确认
                    DUPS_OK_ACKNOWLEDGE：有副本的客户端手动确认，一个消息可多次处理，降低Session消耗，不推荐使用
             */
            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            // 创建目的地，参数为目的地名称
            destination = session.createQueue("test-ptp-Listener");
            // 创建消息生产者，并制定消息发送的目的地
            // 也可以不指定目的地，在发送消息时指定
            messageProducer = session.createProducer(destination);
            // 创建消息对象
            for(int i = 0; i < 100; i++){
                message = session.createTextMessage(datas + "--" + i);
                // 发送消息，失败会抛出异常
                messageProducer.send(message);
                System.out.println("发送完成：" + message);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            if (messageProducer != null) {
                try {
                    messageProducer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        sendMessage("test-ptp-first");
    }
}
