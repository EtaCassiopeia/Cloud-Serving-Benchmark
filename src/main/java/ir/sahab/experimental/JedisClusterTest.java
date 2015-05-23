package ir.sahab.experimental;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <h1>JedisClusterTest</h1>
 * The JedisClusterTest
 *
 * @author Mohsen Zainalpour
 * @version 1.0
 * @since 5/21/15
 */
public class JedisClusterTest {

    //gem install redis

    //create-cluster start
    //create-cluster create

    //create-cluster stop


    private static final int TOTAL_OPERATIONS = 100000;

    private static Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();

    public static void main(String[] args) throws Exception {

//Jedis Cluster will attempt to discover cluster nodes automatically
        jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30001));
        //jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30002));
        //jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30003));
        //jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30004));
        //jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30005));
        //jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30006));
        JedisCluster jc = new JedisCluster(jedisClusterNodes);

        long t = System.currentTimeMillis();
        // withoutPool();
        withPool(jc);
        long elapsed = System.currentTimeMillis() - t;
        System.out.println(((1000 * 2 * TOTAL_OPERATIONS) / elapsed) + " ops");
    }

    private static void withPool(final JedisCluster jedisCluster) throws Exception {
        List<Thread> tds = new ArrayList<Thread>();

        final AtomicInteger ind = new AtomicInteger();
        for (int i = 0; i < 50; i++) {
            Thread hj = new Thread(new Runnable() {
                public void run() {
                    Random random=new Random();
                    for (int i = 0; (i = ind.getAndIncrement()) < TOTAL_OPERATIONS; ) {
                        try {
                           // final String key = "url" + i + random.nextInt();
                            final String key = "url" + i;
                            jedisCluster.set(key + "-value" , key);
                            jedisCluster.incr(key);
                            String result = jedisCluster.get(key);
                            if (result == null || result.length() == 0 )
                                throw new InvalidKeyException("Invalid key");
                            if (i < 10)
                                System.out.println(key + " " + result);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            tds.add(hj);
            hj.start();
        }

        for (Thread t : tds)
            t.join();

    }
}
