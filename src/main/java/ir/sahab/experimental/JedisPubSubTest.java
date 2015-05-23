package ir.sahab.experimental;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JedisPubSubTest {

    static class PubTest implements Runnable {
        private List<JedisPool> pools;

        public PubTest(List<JedisPool> pools) {
            this.pools = pools;
        }

        public void run() {
            int index = 0;
            JedisPool jp = getRandomNode();

            while (index <= 50) {
                Jedis jedis = null;

                try {
                    jedis = jp.getResource();
                    System.out.println("I will publish message on " + jedis.getClient().getHost() + ":" +
                            jedis.getClient().getPort());

                    jedis.publish("hello", "world " + index++);

                    Thread.sleep(1000);
                } catch (JedisConnectionException e) {
                    System.out.println("JedisConnectionException occurred in PubTest");

                    if (jedis != null) {
                        jp.returnBrokenResource(jedis);
                        jedis = null;
                    }

                    // it seems that this node is broken, assign new node
                    jp = getRandomNode();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    if (jedis != null) {
                        jp.returnResource(jedis);
                    }
                }
            }
        }

        private JedisPool getRandomNode() {
            return this.pools.get(new Random().nextInt(pools.size()));
        }
    }

    static class TestPubSub extends JedisPubSub {

        @Override
        public void onMessage(String channel, String message) {
            System.out.println("Message arrived / channel : " + channel + " : message : " + message);
            if (message.endsWith("50")) {
                unsubscribe();
            }

        }

        @Override
        public void onPMessage(String pattern, String channel,
                               String message) {
            System.out.println("Message arrived / channel : " + channel + " : message : " + message);
            if (message.endsWith("50")) {
                unsubscribe();
            }
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
        }

        @Override
        public void onPUnsubscribe(String pattern, int subscribedChannels) {
        }

        @Override
        public void onPSubscribe(String pattern, int subscribedChannels) {
        }

    }

    static class SubTest implements Runnable {
        private List<JedisPool> pools;

        public SubTest(List<JedisPool> pools) {
            this.pools = pools;
        }

        public void run() {
            JedisPool jp = getRandomNode();

            while (true) {
                Jedis jedis = null;

                try {
                    jedis = jp.getResource();
                    System.out.println("I will subscribe on " + jedis.getClient().getHost() + ":" +
                            jedis.getClient().getPort());

                    jedis.subscribe(new TestPubSub(), "hello");
                    break;
                } catch (JedisConnectionException e) {
                    System.out.println("JedisConnectionException occurred in SubTest");

                    if (jedis != null) {
                        jp.returnBrokenResource(jedis);
                        jedis = null;
                    }

                    // it seems that this node is broken, assign new node
                    jp = getRandomNode();
                } finally {
                    if (jedis != null) {
                        jp.returnResource(jedis);
                    }
                }
            }
        }

        private JedisPool getRandomNode() {
            return this.pools.get(new Random().nextInt(pools.size()));
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();

        //Jedis Cluster will attempt to discover cluster nodes automatically
        jedisClusterNodes.add(new HostAndPort("127.0.0.1", 7379));

        JedisCluster jc = new JedisCluster(jedisClusterNodes);

        System.out.println("Currently " + jc.getClusterNodes().size() + " nodes in cluster");

        Map<String, JedisPool> nodeMap = jc.getClusterNodes();

        List<JedisPool> nodePoolList = new ArrayList<JedisPool>(nodeMap.values());
        Collections.shuffle(nodePoolList);

        Thread t1 = new Thread(new PubTest(nodePoolList));
        Thread t2 = new Thread(new SubTest(nodePoolList));

        t2.start();

        Thread.sleep(3000);

        t1.start();

        t1.join();
        t2.join();

        System.out.println("Done...");
    }

}


