package com.sunghwan.kim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;

/**
 * Created by user on 14. 7. 24.
 */
public class UsercountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    int sum;        //전체 합.
    JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
    Jedis jedis = pool.getResource();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 전체 합.
        for(IntWritable value: values){
            sum += value.get();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(sum!=0){
            //redis에 전체 트윗 수 저장. 또한 output으로 전체 트윗 수 저장.
            context.write(new Text("AllTweetNumber"), new IntWritable(sum));
            jedis.set("AllTweetNumber", String.valueOf(sum));
        }
    }
}
