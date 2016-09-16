package com.sunghwan.kim;

import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by user on 14. 7. 25.
 */
public class KeywordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Logger logger = Logger.getLogger(KeywordcountMapper.class);

    int keywordNumber;
    ArrayList keywordList;
    String[] keywordToken;

    // 형태소 분석을 위한 solr 설정. 은전한닢 형태소 분석기 사용.
    HttpSolrServer solr;
    BinaryResponseParser responseParser;
    FieldAnalysisRequest request;
    String urlString = "http://localhost:8983/solr";
    NamedList<Object> result;
    String middleResult = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // counter를 통해 값을 받아온다.
        Configuration conf = context.getConfiguration();
        keywordNumber = Integer.parseInt(conf.get("keywordNumber").toString());    //키워드 숫자. 이를 통해 각 카운터와 redis에  접근
        // 받아온 키워드 리스트
        keywordList = new ArrayList();
        for (int i = 0; i < keywordNumber; i++) {
            keywordToken = conf.get(String.valueOf(i)).split("\t");
            keywordList.add(keywordToken[0]);
        }

        //형태소 분석기 셋팅.
        solr = new HttpSolrServer(urlString);
        responseParser = new BinaryResponseParser();
        request = new FieldAnalysisRequest();
        request.addFieldType("text_ko");
        request.addFieldName("text");


    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] valueTokens = value.toString().split("\t"); // value : 아이디 스크린아이디 시간 텍스트
        // 길이 확인.
        if (valueTokens.length == 4) {
            for (int i = 0; i < keywordNumber; i++) {
                //tweet 내용 text를 형태소 분석 요청.
                request.setQuery(valueTokens[3]);
                try {
                    //결과 result에 저장.
                    result = solr.request(request, responseParser);
                } catch (SolrServerException e) {
                    e.printStackTrace();
                }
                // 형태소 분석 결과가 null이 아닐 경우
                if(result.toString().split("text=")!=null){
                    // 'text =~~~~,raw_bytes=~~' 의 형태로 날라온다.
                    String[] solrResult = result.toString().split("text=");
                    //해당 트윗의 형태소 분석 결과.
                    for (String token : solrResult) {
                        if (middleResult != null) { //중간 결과값이 있을 경우
                            token += middleResult;
                            middleResult = null;
                        }

                        if (!token.contains(",raw_bytes=")) {   //raw_bytes가 포함되어 있지 않다면
                            middleResult = " " + token; //중간 결과 값 생성.
                        } else {    //raw_bytes=가 포함되어 있다면 raw_bytes= 앞에 형태소가 나옴.
                            String[] solrResultToken = token.split(",raw_bytes=");      //raw_bytes로 slit
                            if (solrResultToken.length == 2) {      //
                                if (solrResultToken[0].equals(keywordList.get(i).toString())) {//형태소 분석한 단어가 keyword와 같다면
                                    context.write(new Text(keywordList.get(i).toString()), new IntWritable(1)); //해당 키워드에 value 1 추가.
                                }
                            }
                        }
                    }
                }
            }
        }

    }
    //형태소 분석기 종료
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        solr.shutdown();
    }
}
