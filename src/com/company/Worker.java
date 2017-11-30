package com.company;

import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;

public class Worker {
    ///MyType myobject = gson.fromJson(jsonSource, MyType.class);
    private final static String QUEUE_NAME = "Task_Queue";

    public static void main(String[] args) throws java.io.IOException,
            java.lang.InterruptedException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        boolean durable = true;
        //Durable =  make sure that RabbitMQ will never lose our queue (even if queue shuts down)


        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // This tells RabbitMQ not to give more than one message to a worker at a time
        int numOfMessages = 1;
        channel.basicQos(numOfMessages);


        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");

                try{
                    //doStuff(message);
                    dojsonStuff(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    // let the QUEUE know that the message has been fully delivered. (ack it).
                    channel.basicAck(envelope.getDeliveryTag(), false);
                    System.out.println(" [x] Done");
                }

            }
        };
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, consumer);

    }

    private static void doStuff(String task) throws InterruptedException {


        for (char ch : task.toCharArray()) {
            if (ch == '.') { System.out.println("Character is a '.' Doing work"); Thread.sleep(1000);}
        }

    }
    private static void dojsonStuff(String task) throws InterruptedException{

        System.out.println(task);

        Gson gson = new Gson();
        Type type = new TypeToken<Map<String, Object>>(){}.getType();
        Map<String, Object> map = gson.fromJson(task,type);

        System.out.println("[x] Printing Incident Number: " + map.get("Incident Number"));

    }

}