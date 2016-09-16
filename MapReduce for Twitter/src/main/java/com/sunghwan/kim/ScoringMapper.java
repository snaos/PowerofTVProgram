package com.sunghwan.kim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by user on 14. 6. 1.
 */
public class ScoringMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    int keywordNumber;
    ArrayList keywordList;
    String[] keywordToken;
    int k;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        keywordNumber = Integer.parseInt(conf.get("keywordNumber").toString());    //키워드 숫자
        keywordList = new ArrayList();

        for(int i=0;i<keywordNumber;i++){
            keywordToken = conf.get(String.valueOf(i)).split("\t");
            keywordList.add(keywordToken[0]);
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //keyword   ID  Relevance_sorce 순으로 value가 들어옴.
        String[] valueToken = value.toString().split("\t");

        for(int i = 0;i<keywordNumber;i++){
            if(valueToken[0].equals(keywordList.get(i).toString())){
                k = i;
            }
        }

        context.write(new IntWritable(k), new Text(valueToken[1] + "-" + valueToken[2]));
        //keyword를 key로 하여 value에는 ID와 Relevance_score 저장.
    }
}
