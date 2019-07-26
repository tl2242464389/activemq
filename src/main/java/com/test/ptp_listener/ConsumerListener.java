package com.test.ptp_listener;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @Description:
 * @Author: tl
 * @Date: 2019-06-02 10:30
 * @Version: 1.0
 */
public class ConsumerListener {
    // 定义成员变量接受匿名内部类的数据
    String result = "";

    public String receiveMessage(){
        ConnectionFactory factory = null;
        Connection connection = null;
        Session session = null;
        Destination destination = null;
        MessageConsumer messageConsumer = null;
        Message tx_message = null;

        try {
            factory = new ActiveMQConnectionFactory("admin", "admin", "tcp://192.168.76.136:61616");
            connection = factory.createConnection();
            // 消费者必须启动连接
            connection.start();
            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            destination = session.createQueue("test-ptp-Listener");
            // 创建消费者对象
            messageConsumer = session.createConsumer(destination);

            // 注册监听器，注册成功后，消息队列中的消息发生变化会自动触发监听器代码，接收消息并处理
            messageConsumer.setMessageListener(new MessageListener() {
                /*
                    监听器一旦注册，永久有效 - Consumer线程不关闭的情况
                    处理方式：只要有消息未处理，自动调用onMessage
                    监听器可以在多个Consumer中注册多个，类似于集群效果
                    ActiveMQ自动循环调用监听器实现并行处理消息
                    message：本次拉去的未处理的消息
                 */
                @Override
                public void onMessage(Message message) {
                    try {
                        // 消息确认方法，代码Consumer已经接受到消息，确定后ActiveMQ删除该条消息
                        message.acknowledge();
                        result = ((TextMessage) message).getText();
                        System.out.println(result);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
            // 阻塞代码，保证Listener监听代码未结束，主线程结束，监听代码也会停止
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(messageConsumer != null){
                try {
                    messageConsumer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if(session != null){
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if(connection != null){
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
        String result = new ConsumerListener().receiveMessage();
        // System.out.println("接收的消息内容为：" + result);
    }
}
