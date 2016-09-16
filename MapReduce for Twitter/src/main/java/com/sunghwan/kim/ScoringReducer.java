package com.sunghwan.kim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
/**
 * Created by user on 14. 6. 1.
 */
public class ScoringReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
    HashMap<String, Integer> map = new HashMap<String, Integer>();            //keyword를 언급한 ID를 저장할 hashmap
    JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
    Jedis jedis = pool.getResource();

    double keywordRelevance = 0;                  //relevance score을 더하기 위한 변수

    int keywordNumber;


    ArrayList keywordList;
    ArrayList IncludeKeywordTweetList;
    String[] keywordToken;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        keywordNumber = Integer.parseInt(conf.get("keywordNumber").toString());    //키워드 숫자
        IncludeKeywordTweetList = new ArrayList();
        keywordList = new ArrayList();
        for(int i=0;i<keywordNumber;i++){
            keywordToken = conf.get(String.valueOf(i)).split("\t");
            keywordList.add(keywordToken[0]);
            IncludeKeywordTweetList.add(jedis.get("I"+String.valueOf(i)));

            map.put("n"+keywordToken[0],Integer.parseInt(conf.get("n"+String.valueOf(i))));
        }
    }
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key = keyword number
        // value = ID  Relevance_sorce 순으로 value가 들어옴.
        int sum = 0;//keyword포함한 유저수
        //같은 유저가 여러번 언급한 경우를 가리기 위해서 각 유저당 1번씩 카운트
        for (Text value : values) {
            String[] valueToken = value.toString().split("-");                 // value값을 나눠줌.
            if (!map.containsKey(keywordList.get(key.get()).toString() + valueToken[0])) {                // hashmap에 해당 아이디가 저장되어 있지 않을 경우
                map.put(key.toString() + valueToken[0], new Integer(1));          // hashmap에 해당 아이디를 key로 갖는 값을 set
                sum++;                                  // 언급한 유저수 증가
            }
            keywordRelevance += Double.parseDouble(valueToken[1]);    //해당 키워드의 Relevance_score값을 더해줌.
        }
        context.write(null, new Text(map.get("n"+keywordList.get(key.get()).toString())+"\t"+keywordList.get(key.get()).toString()+"\t" + keywordRelevance + "\t" + String.valueOf(sum) + "\t" + IncludeKeywordTweetList.get(key.get())));
        //keyword를 key로 하고 value에 해당하는 relevance_score와 언급한 유저수, 언급한 트윗수
        //(keyword number(키워드의 고유번호)    keyword, Relevance score     number of user      number of tweet)
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}

