package com.sunghwan.kim;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by user on 2014-08-09.
 */
public class ScoreSumMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //  keyword number(고유번호)    keyword   Relevance score     number of user      number of tweet
        String[] valueToken = value.toString().split("\t");
        context.write(new Text(valueToken[0]),new Text(value));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
