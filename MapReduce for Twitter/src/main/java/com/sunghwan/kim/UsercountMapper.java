package com.sunghwan.kim;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by user on 14. 7. 24.
 */
public class UsercountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }
    // 아무 작업 하지 않고 tweet 수를 센다.
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new Text("key"),new IntWritable(1));      //tweet갯수 세는 거라서 그냥 각 라인당으로 카운트
    }
}
