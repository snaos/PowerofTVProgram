package com.sunghwan.kim;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by user on 2014-08-02.
 */

public class InputMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //value = 라인넘버  keyword time    고유번호
        //들어온 값 그대로 reducer로 넘긴다.
        String[] valueToken = value.toString().split("/");
        //빈공간 확인
        if (valueToken.length==4) {
            context.write(new Text("key"), new Text(valueToken[0]+"\t"+valueToken[1]+"\t"+valueToken[2]+"\t"+valueToken[3]));
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
