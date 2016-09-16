package com.sunghwan.kim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by user on 14. 5. 2.
 */
public class RelevanceReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
    JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
    Jedis jedis = pool.getResource();

    int keywordNumber;      //키워드 숫자

    int AllTweetNumber;                  //모든 트윗들의 수
    String[] keyToken;
    double temporal_relevance;
    double textual_relevance;   //Textual relavance를 정장하는 변수
    double IDF;                 //IDF값을 저장하는 변수
    int TF;                     //TF값을 저장하는 변수

    ArrayList keywordList;
    ArrayList IncludeKeywordTweetList;
    String[] keywordToken;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        AllTweetNumber = Integer.parseInt(jedis.get("AllTweetNumber"));     //전체 트윗 수를 redis에서 가져온다.
        Configuration conf = context.getConfiguration();
        keywordNumber = Integer.parseInt(conf.get("keywordNumber").toString());    //키워드 숫자

        IncludeKeywordTweetList = new ArrayList();
        keywordList = new ArrayList();
        for(int i=0;i<keywordNumber;i++){
            keywordToken = conf.get(String.valueOf(i)).split("\t");
            keywordList.add(keywordToken[0]);
            IncludeKeywordTweetList.add(jedis.get("I"+String.valueOf(i)));
        }
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        //keyword와 TF, ID값을 key로 설정, temporal relevance값을 value로 갖는다.
        keyToken = key.toString().split(","); //key에는 keyword와 TF와 ID가 저장되어 있다.  마지막엔 시간.

        int k = 0;

        for (int i = 0; i<keywordNumber; i++) {
            if (keywordList.get(i).toString().equals(keyToken[0])) {
                k = i;
                break;
            }
        }

        TF = Integer.parseInt(keyToken[1]);     //TF 값.
        IDF = Math.log10(AllTweetNumber / Double.parseDouble(IncludeKeywordTweetList.get(k).toString()));          //IDF값 계산
        // IDF는 log10(전체 트윗 수 / 해당 키워드를 포함한 트윗 수)
        for (DoubleWritable value : values) {               //각 value마다(각 트윗마다) relevance score 계산
            temporal_relevance = value.get();      //temporal relevance값 설정.

            textual_relevance = TF * IDF;             //textual relevance값을 구함.

            double relevance_score = textual_relevance / temporal_relevance;

            context.write(new Text(keyToken[0] + "\t" + keyToken[2] + "\t" + relevance_score), null);
            //keyword   ID  Relevance_score 순으로 저장
            //(keyword      ID      Relevance score, )
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
