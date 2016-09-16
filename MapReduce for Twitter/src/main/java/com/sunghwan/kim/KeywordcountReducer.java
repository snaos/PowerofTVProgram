package com.sunghwan.kim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by user on 14. 7. 25.
 */
public class KeywordcountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    Logger logger = Logger.getLogger(KeywordcountReducer.class);

    JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
    Jedis jedis = pool.getResource();

    int keywordNumber;
    ArrayList keywordList;
    String[] keywordToken;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //counter 설정
        Configuration conf = context.getConfiguration();
        keywordNumber = Integer.parseInt(conf.get("keywordNumber").toString());    //키워드 숫자
        keywordList = new ArrayList();
        for(int i=0;i<keywordNumber;i++){
            keywordToken = conf.get(String.valueOf(i)).split("\t");
            keywordList.add(keywordToken[0]);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for(IntWritable value: values){
            sum += value.get();             //키워드의 값들을 더함(키워드를 언급한 트윗들의 총합)
        }

        //redis에 I1, I2와 같은 형태로 해당 키워드번호의 총 합 값(해당 키워드를 포함한 트윗 수)을 저장함.
        for (int i = 0; i<keywordNumber; i++) {
            if (key.toString().equals(keywordList.get(i).toString())) {
                jedis.set("I"+String.valueOf(i),String.valueOf(sum));
                break;
            }
        }
        //결과 출력.
        context.write(key, new IntWritable(sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
