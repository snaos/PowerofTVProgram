package com.sunghwan.kim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.FieldAnalysisRequest;
import org.apache.solr.common.util.NamedList;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


/**
 * Created by user on 14. 5. 2.
 */
public class RelevanceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    ArrayList keywordList;
    ArrayList TimeList;

    JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
    Jedis jedis = pool.getResource();

    //시간 저장 token
    String[] tweetYearTokens = new String[5];           //ex) 2014-05-09 , 19:53:49.333
    String[] tweetDayTokens = new String[5];            //ex) 2014 , 05 , 09
    String[] tweetTimeTokens = new String[5];           //ex) 19 , 53 , 49.333

    //프로그램의 방송시간들의 토큰
    ArrayList<Integer> month;
    ArrayList<Integer> day;
    ArrayList<Integer> startHour;
    ArrayList<Integer> startMinute;
    ArrayList<Integer> endHour;
    ArrayList<Integer> endMinute;

 //   Logger logger = Logger.getLogger(RelevanceMapper.class);

    //Temporal Score
    double score;

    int TF;                 //포함된 단어의 수, TF = 하나의 트윗에서 해당 프로그램 언급 횟수
    HashMap<Integer,String[]> timeToken;

    String[] keywordToken;      //counter로 넘겨받은 키워드들 저장
    int keywordNumber;
    int j;
    int k;

    //형태소 분석기 사용.
    HttpSolrServer solr;
    BinaryResponseParser responseParser;
    FieldAnalysisRequest request;
    String urlString = "http://localhost:8983/solr";
    NamedList<Object> result;
    String middleResult = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        timeToken = new HashMap<Integer, String[]>();
        j=0;
        k=0;
        Configuration conf = context.getConfiguration();
        keywordNumber = Integer.parseInt(conf.get("keywordNumber").toString());    //키워드 숫자
        keywordList = new ArrayList();
        TimeList = new ArrayList();
        month = new ArrayList();
        day = new ArrayList();
        startHour = new ArrayList();
        startMinute = new ArrayList();
        endHour = new ArrayList();
        endMinute = new ArrayList();
        //각 키워드들을 받아온다.
        for(int i=0;i<keywordNumber;i++){
            keywordToken = conf.get(String.valueOf(i)).split("\t");
            keywordList.add(keywordToken[0]);
            //TimeList.add(conf.get("t"+String.valueOf(i)));
            TimeList.add(jedis.get("t"+i)); //redis에서 시간 정보를 받아온다.
            //logger.error("in Relevance Mapper, i = "+i+" conf.get(\"t\"+String.valueOf(i)) = "+conf.get("t"+String.valueOf(i))+"\n");
        }

        for(int i=0;i<keywordNumber;i++) {
            //시간 설정.
            timeToken.put(j,TimeList.get(k).toString().split("-"));         //2014, 06, 28, 22:30-23:30
            String[] timeTokenGet = timeToken.get(j);
            month.add(Integer.parseInt(timeTokenGet[1]));
            day.add(Integer.parseInt(timeTokenGet[2]));

            timeToken.put(j+1,timeTokenGet[3].toString().split("~"));       //22:30, 23:30
            timeTokenGet = timeToken.get(j+1);
            timeToken.put(j+2,timeTokenGet[0].toString().split(":"));       //ex) 21, 30
            timeToken.put(j+3,timeTokenGet[1].toString().split(":"));       //ex)22, 30
            timeTokenGet = timeToken.get(j+2);
            startHour.add(Integer.parseInt(timeTokenGet[0]));
            startMinute.add(Integer.parseInt(timeTokenGet[1]));
            timeTokenGet = timeToken.get(j+3);
            endHour.add(Integer.parseInt(timeTokenGet[0]));
            endMinute.add(Integer.parseInt(timeTokenGet[1]));
            k+=1;           //키워드 토큰 증가
            j += 4;         //시간 토큰 증가.
        }

        solr = new HttpSolrServer(urlString);
        responseParser = new BinaryResponseParser();
        request = new FieldAnalysisRequest();
        request.addFieldType("text_ko");
        request.addFieldName("text");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        TF = 0;
        String[] valueTokens = value.toString().split("\t"); // value : 아이디 스크린아이디 시간 텍스트
        // valueTokens = {아이디, 스크린아이디, 시간, 텍스트}
        if (valueTokens.length == 4) {
            for(int i=0;i<keywordNumber;i++) {
                request.setQuery(valueTokens[3]);       //텍스트 형태소 분석
                try {
                    result = solr.request(request, responseParser);
                } catch (SolrServerException e) {
                    e.printStackTrace();
                }
                // keywordcountMapper와 같음.
                String[] solrResult = result.toString().split("text=");
                for (String token : solrResult) {
                    if (middleResult != null) {
                        token += middleResult;
                        middleResult = null;
                    }
                    if (!token.contains(",raw_bytes=")) {
                        middleResult = " "+token;
                    } else {
                        String[] solrResultToken = token.split(",raw_bytes=");
                        if (solrResultToken.length == 2) {
                            if (solrResultToken[0].equals(keywordList.get(i).toString())) {//형태소 분석한 단어가 keyword와 같다면

                                TF++;       //TF 증가. 언급횟수 증가

                                //트위터의 트윗 시간 토큰
                                tweetYearTokens = valueTokens[2].toString().split(" "); //ex) 2014-05-09 , 19:53:49.333
                                tweetDayTokens = tweetYearTokens[0].toString().split("-");   //ex) 2014 , 05 , 09
                                tweetTimeTokens = tweetYearTokens[1].toString().split(":");  //ex) 19 , 53 , 49.333

                                int tweetDay = Integer.parseInt(tweetDayTokens[2]);

                                int mmonth = month.get(i);
                                int mday = day.get(i);
                                int mstartHour = startHour.get(i);
                                int mstartMinute = startMinute.get(i);
                                int mendHour = endHour.get(i);
                                int mendMinute = endMinute.get(i);

                                //해당 트윗의 시점 확인. 방송일을 기준으로
                                if (mmonth != Integer.parseInt(tweetDayTokens[1])) {  //프로그램 방영 달과 트윗의 달이 다른 경우
                                    if ((mmonth == 12 || mmonth == 1) && (Integer.parseInt(tweetDayTokens[1]) == 12 || Integer.parseInt(tweetDayTokens[1]) == 1)) {
                                        //년도가 바뀐 경우
                                        if (mmonth > Integer.parseInt(tweetDayTokens[1])) {
                                            tweetDay += 31;
                                        } else {
                                            mday += 31;
                                        }
                                    } else {       //년도가 같은 경우 두개를 비교하여 날짜를 더해준다.
                                        if (mmonth > Integer.parseInt(tweetDayTokens[1])) {
                                            if (Integer.parseInt(tweetDayTokens[1]) == 1 || Integer.parseInt(tweetDayTokens[1]) == 3 || Integer.parseInt(tweetDayTokens[1]) == 5 || Integer.parseInt(tweetDayTokens[1]) == 7 ||
                                                    Integer.parseInt(tweetDayTokens[1]) == 8 || Integer.parseInt(tweetDayTokens[1]) == 10 || Integer.parseInt(tweetDayTokens[1]) == 12) {
                                                mday += 31;
                                            } else if (Integer.parseInt(tweetDayTokens[1]) == 2) {
                                                mday += 28;
                                            } else {
                                                mday += 30;
                                            }
                                        } else {
                                            if (Integer.parseInt(tweetDayTokens[1]) == 5 || Integer.parseInt(tweetDayTokens[1]) == 7 ||
                                                    Integer.parseInt(tweetDayTokens[1]) == 8 || Integer.parseInt(tweetDayTokens[1]) == 10 || Integer.parseInt(tweetDayTokens[1]) == 12) {
                                                tweetDay += 30;
                                            } else if (Integer.parseInt(tweetDayTokens[1]) == 3) {
                                                tweetDay += 28;
                                            } else {
                                                tweetDay += 31;
                                            }
                                        }
                                    }
                                }

                                int programStartTime = ((mday * 24 + mstartHour) * 60 + mstartMinute) * 60;        //프로그램 시작시간을 수치화 (초로 변환)
                                int programEndTime = ((mday * 24 + mendHour) * 60 + mendMinute) * 60;              //프로그램 종료시간을 수치화 (초로 변환)
                                int valueTime = (((tweetDay * 24 + Integer.parseInt(tweetTimeTokens[0])) * 60) + Integer.parseInt(tweetTimeTokens[1])) * 60;
                                //트윗의 시간을 수치화 (초로 변환)

                                if (programStartTime > valueTime) {         //프로그램 시작 전 (log10(시작시간 - 트윗 작성시간 + 0.1))
                                    score = Math.log10(programStartTime - valueTime + 0.1);
                                } else if (programEndTime < valueTime) {      //프로그램 시작 후
                                    score = Math.log10(valueTime - programEndTime + 0.1);
                                } else                                    //프로그램 도중 트윗한 경우
                                    score = 0.04139268515;

                                context.write(new Text(keywordList.get(i).toString() + "," + TF + "," + valueTokens[0]), new DoubleWritable(score));
                                //keyword와 TF, ID값을 key로 설정, temporal relevance값을 value로 갖는다.
                                TF=0;
                            }
                        }
                    }
                }
            }
        }
    }
}

