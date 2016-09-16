package com.sunghwan.kim;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by user on 2014-08-02.
 */
public class InputReducer extends Reducer<Text,Text,Text,Text>{
    //redis 사용
    JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
    Jedis jedis = pool.getResource();
    // logger 사용 시.
    //Logger logger = Logger.getLogger(InputReducer.class);
    //couter 설정
    Counter keywordNumber;
    //arrayList 설정
    ArrayList<String> keywordList;
    ArrayList<String> timeList;
    ArrayList<String> numbering;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //couter 설정 후 couter 초기 설정.
        keywordNumber = context.getCounter("keywordNumberGroup".toUpperCase(),"keywordNumber");
        keywordNumber.setValue(0);
        // arrayList 설정
        keywordList = new ArrayList();
        timeList = new ArrayList();
        numbering = new ArrayList();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //key는 라인넘버, value는 라인넘버    키워드 시간  고유번호()
        for(Text value:values) {        //모두 'key'라는 Key를 설정해서  배열로 'key'가 mapper에서 넘어왔다.
            String[] valueToken = value.toString().split("\t");     //"t"으로 토크닝
            keywordNumber.increment(1);         //키워드 번호 증가
            //jedis.set(String.valueOf(keywordNumber.getValue()), valueToken[1].toString() + "\t" + valueToken[2].toString());
            //각각 리스트에 같은 n번째 키워드에 관한 내용은 같은 n번째에 들어있다.
            keywordList.add(valueToken[1].toString());  // 키워드 리스트에 키워드 추가
            timeList.add(valueToken[2].toString());     // 시간 리스트에 시간 추가
            numbering.add(valueToken[3].toString());    // 고유 번호 추가.
            //reducer 결과 값 저장. 결과 값의 key는 고유번호, value는 라인넘버  keyword time
            context.write(new Text(String.valueOf(keywordNumber.getValue())), new Text(valueToken[0].toString() + "\t" + valueToken[1].toString() + "\t" + valueToken[2].toString()));
        }

        //저장 : keyword넘버  line넘버  key time (그냥 keyword랑 line넘버 비교하려고 둘다 출력)
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //각각 키워드에 관한 counter값 셋팅과 redis에 해당 값 저장.
        for(int i =0;i<keywordNumber.getValue();i++){
            // keywordList라는 counter에 key는 keyword의 이름, 값은 몇 번째 keyword 인지 확인을 위한 i.
            context.getCounter("keywordList".toUpperCase(),keywordList.get(i)).setValue(i);
            // redis에 시간을 나타내는 t라는 값에 i값을 더해 i번째에 있는 트윗 시간을 저장.
            jedis.set("t"+i,timeList.get(i));
            // numbering이라는 counter에 i라는 key로 접근하며 value는 고유 번호 값
            context.getCounter("numbering".toUpperCase(),String.valueOf(i)).setValue(Integer.parseInt(numbering.get(i)));

            //context.getCounter("timeList".toUpperCase(),timeList.get(i)).setValue(i);
        }
    }
}
