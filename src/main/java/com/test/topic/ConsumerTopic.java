package com.test.topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @Description:
 * @Author: tl
 * @Date: 2019-06-02 10:30
 * @Version: 1.0
 */
public class ConsumerTopic {

    public static String receiveMessage() {
        String result = "";
        ConnectionFactory factory = null;
        Connection connection = null;
        Session session = null;
        Destination destination = null;
        MessageConsumer messageConsumer = null;
        Message message = null;

        try {
            factory = new ActiveMQConnectionFactory("admin", "admin", "tcp://192.168.76.136:61616");
            connection = factory.createConnection();
            // 消费者必须启动连接
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // 创建发布/订阅模式的目的地
            destination = session.createTopic("test-topic");
            // 创建消费者对象
            messageConsumer = session.createConsumer(destination);
            // 获取队列消息，receive是一个主动拉去消息的方法，测试使用，一种阻塞方法
            message = messageConsumer.receive();
            // 处理消息
            result = ((TextMessage) message).getText();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (messageConsumer != null) {
                try {
                    messageConsumer.close();
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
        return result;
    }

    public static void main(String[] args) {
        String result = receiveMessage();
        System.out.println("接收的消息内容为：" + result);
    }
}

