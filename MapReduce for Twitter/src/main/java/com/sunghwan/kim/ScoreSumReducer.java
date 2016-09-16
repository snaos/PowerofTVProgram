package com.sunghwan.kim;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.mortbay.jetty.security.SSORealm;

import java.io.IOException;

/**
 * Created by user on 2014-08-09.
 */
public class ScoreSumReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // value :  keyword number(고유번호)    keyword   Relevance score     number of user      number of tweet
       String sumOfKeyword = "";
       double sumOfRelevance = 0;
       int sumOfUser = 0;
        int sumOfTweet = 0;

       for(Text value : values){
           String[] valueToken = value.toString().split("\t");
           sumOfKeyword += valueToken[1]+" ";
           sumOfRelevance += Double.valueOf(valueToken[2]);
           sumOfUser += Integer.valueOf(valueToken[3]);
           sumOfTweet += Integer.valueOf(valueToken[4]);
       }
        context.write(new Text(sumOfKeyword),new Text("\t"+sumOfRelevance+"\t"+sumOfUser+"\t"+sumOfTweet));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
