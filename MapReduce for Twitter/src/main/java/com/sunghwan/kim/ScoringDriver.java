package com.sunghwan.kim;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * Created by user on 14. 5. 2.
 */
// Configured 상속, Tool 인터페이스 구현
public class ScoringDriver extends Configured implements Tool {

    private static String[][] requirements = {                          //요구사항 부족시 명시
            {"input","Job의 입력 경로를 설정해주세요."},
            {"output1","output1 경로를 설정해주세요."},
            {"output2","output2 경로를 설정해주세요."},
            {"output3","output3 경로를 설정해주세요."},
            {"output4","output4 경로를 설정해주세요."},
            {"output5","output5 경로를 설정해주세요."},
            {"output6","output6 경로를 설정해주세요."},
    };

    //
    private static Options getOptions(){
        Options options = new Options();
        options.addOption("i", "input", true, "분석 데이터 입력 경로 (필수)");
        options.addOption("o1", "output1", true, "output 경로 (필수)");
        options.addOption("o2", "output2", true, "output 경로 (필수)");
        options.addOption("o3", "output3", true, "output 경로 (필수)");
        options.addOption("o4", "output4", true, "output 경로 (필수)");
        options.addOption("o5", "output5", true, "output 경로 (필수)");
        options.addOption("o6", "output6", true, "output 경로 (필수)");

        return options;
    }
    public static void main(String[] args) throws Exception {
        //워드 카운트를 위한 pig 사용.
        //이곳에서 워드는 입력한 TV프로그램 이름.
        String pigCommand = "pig -x mapreduce pig-script";

        // pig command 터미널에 입력
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec(pigCommand);
        //  pig가 수행된 후에야 drvier 실행
        synchronized (process) {
            process.wait();
        }
        //driver 실행
        int res = ToolRunner.run(new ScoringDriver(), args);
        System.exit(res);
    }
    //override anotation
    @Override
    public int run(String[] strings) throws  Exception{
        //각각 잡 정의 (실행 순서대로)
        Job InputJob = new Job();
        Job UsercountJob = new Job();
        Job KeywordcountJob = new Job();
        Job relevanceJob = new Job();
        Job scoringJob = new Job();
        Job summingJob = new Job();

        //각각 input과 output이 설정되어야 한다.
        if(!parseArgument(relevanceJob, scoringJob,UsercountJob,KeywordcountJob,InputJob,summingJob, strings)){
            return -1;
        }
        // inputJob input path 설정 , pig 결과물로 각 tv프로그램과 해당 tweet들에서 해당 단어 언급횟수가 나옴.
        FileInputFormat.setInputPaths(InputJob,"/input");
        // inputJob에서는 tv프로그램들 keyword를 분석
        //input mapper, reducer 설정
        InputJob.setJarByClass(ScoringDriver.class);
        InputJob.setMapperClass(InputMapper.class);
        InputJob.setReducerClass(InputReducer.class);
        // 각 input output
        InputJob.setMapOutputKeyClass(Text.class);
        InputJob.setMapOutputValueClass(Text.class);
        InputJob.setOutputKeyClass(Text.class);
        InputJob.setOutputValueClass(Text.class);
        // reducer 갯수
        InputJob.setNumReduceTasks(3);
        // 수행 결과 기다림.
        InputJob.waitForCompletion(true);
        // InputJob에서 couter를 받아옴.
        Counters counters = InputJob.getCounters();
        // counter 그룹들.
        CounterGroup keywordNumberGroup = counters.getGroup("keywordNumberGroup".toUpperCase());
        CounterGroup keywordListGroup = counters.getGroup("keywordList".toUpperCase());
        //CounterGroup timeListGroup = counters.getGroup("timeList".toUpperCase());
        CounterGroup numberingGroup = counters.getGroup("numbering".toUpperCase());
        //각 그룹에서 couter을 받아와 각각 다른 job으로 넘겨준다.
        for(Counter counter : keywordNumberGroup){
            String counterName = counter.getName();
            Long keywordNumber = counter.getValue();
            // 각각 다른 job으로
            KeywordcountJob.getConfiguration().set(counterName,keywordNumber.toString());
            relevanceJob.getConfiguration().set(counterName,keywordNumber.toString());
            scoringJob.getConfiguration().set(counterName,keywordNumber.toString());
        }
        // 같은 작업 반복.
        for(Counter counter :keywordListGroup){
            String counterName = counter.getName();
            Long keywordNumber = counter.getValue();
            KeywordcountJob.getConfiguration().set(keywordNumber.toString(),counterName);
            relevanceJob.getConfiguration().set(keywordNumber.toString(),counterName);
            scoringJob.getConfiguration().set(keywordNumber.toString(),counterName);
        }
//        for(Counter counter :timeListGroup){
//            String counterName = counter.getName();
//            Long timeNumber = counter.getValue();
//            KeywordcountJob.getConfiguration().set("t"+timeNumber.toString(),counterName);
//            relevanceJob.getConfiguration().set("t"+timeNumber.toString(),counterName);
//            scoringJob.getConfiguration().set("t"+timeNumber.toString(),counterName);
//        }
        //같은 작업 반복. 하지만 key 셋팅을 numbering을 뜻하는 'n'과 해당 번호, value는 numbering 값
        for (Counter counter :numberingGroup){
            String counterName = counter.getName();
            Long Number = counter.getValue();
            scoringJob.getConfiguration().set("n"+counterName,String.valueOf(Number));
            KeywordcountJob.getConfiguration().set("n"+counterName,String.valueOf(Number));
            relevanceJob.getConfiguration().set("n"+counterName,String.valueOf(Number));
        }

        //UsercountJob. tweet수를 구한다. input은 tweet들.
        //job의 map,reduce,driver 설정
        UsercountJob.setJarByClass(ScoringDriver.class);
        UsercountJob.setMapperClass(UsercountMapper.class);
        UsercountJob.setReducerClass(UsercountReducer.class);
        //Job의 Output 형태  명시
        UsercountJob.setMapOutputKeyClass(Text.class);
        UsercountJob.setMapOutputValueClass(IntWritable.class);
        UsercountJob.setOutputKeyClass(Text.class);
        UsercountJob.setOutputValueClass(IntWritable.class);
        //Reducer 갯수
        UsercountJob.setNumReduceTasks(5);
        //수행
        UsercountJob.waitForCompletion(true);

        //KeywordCountJob. tweet에서 키워드의 발현 빈도 측정
        KeywordcountJob.setJarByClass(ScoringDriver.class);
        KeywordcountJob.setMapperClass(KeywordcountMapper.class);
        KeywordcountJob.setReducerClass(KeywordcountReducer.class);

        KeywordcountJob.setMapOutputKeyClass(Text.class);
        KeywordcountJob.setMapOutputValueClass(IntWritable.class);
        KeywordcountJob.setOutputKeyClass(Text.class);
        KeywordcountJob.setOutputValueClass(IntWritable.class);

        KeywordcountJob.setNumReduceTasks(5);

        KeywordcountJob.waitForCompletion(true);



        //relevanceJob. TF/IDF에서의 relevance value를 구한다. input은 tweet들
        // TF = text frequency, 해당 해당 트윗에서 키워드 언급 빈도
        // IDF = inverse document frequency, 키워드를 포함한 트윗의 출현 빈도
        // TF * IDF = Textual Relevence.
        // Temporal Relevence = 해당 트윗과 방송 시간에 따른 영향력 상관 관계
        // 트윗 작성시간 < 프로그램 시작시간 -> log10(프로그램 시작시간 - 트윗 작성시간 + 0.1)
        //  프로그램종료 시간 < 트윗 작성시간 -> log10(트윗 시간 - 프로그램 종료시간 + 0.1)
        // 프로그램 방영중 트윗 = log10(1+0.10 = 약04139268515;
        // 최종 스코어 =   Textual Relevence( TF * IDF ) / Temporal Relevence

        //job의 map,reduce,driver 설정
        relevanceJob.setJarByClass(ScoringDriver.class);
        relevanceJob.setMapperClass(RelevanceMapper.class);
        relevanceJob.setReducerClass(RelevanceReducer.class);

        //Job의 Output 형태  명시
        relevanceJob.setMapOutputKeyClass(Text.class);
        relevanceJob.setMapOutputValueClass(DoubleWritable.class);
        relevanceJob.setOutputKeyClass(Text.class);
        relevanceJob.setOutputValueClass(NullWritable.class);
        //Reduce 갯수 설정
        relevanceJob.setNumReduceTasks(5);

        relevanceJob.waitForCompletion(true);           //완료 될 때 까지 wait

        // ScoringJob. 트윗들의 최종 값.
        scoringJob.setJarByClass(ScoringDriver.class);
        scoringJob.setMapperClass(ScoringMapper.class);
        scoringJob.setReducerClass(ScoringReducer.class);
        //Job의 Output 형태 Text Int형 명시
        scoringJob.setMapOutputKeyClass(IntWritable.class);
        scoringJob.setMapOutputValueClass(Text.class);
        scoringJob.setOutputKeyClass(NullWritable.class);
        scoringJob.setOutputValueClass(Text.class);
        //Reduce 갯수 설정
        scoringJob.setNumReduceTasks(5);

        scoringJob.waitForCompletion(true);
        //최종 summingJob
        summingJob.setJarByClass(ScoringDriver.class);
        summingJob.setMapperClass(ScoreSumMapper.class);
        summingJob.setReducerClass(ScoreSumReducer.class);
        //Job의 Output 형태 Text Int형 명시
        summingJob.setMapOutputKeyClass(Text.class);
        summingJob.setMapOutputValueClass(Text.class);
        summingJob.setOutputKeyClass(Text.class);
        summingJob.setOutputValueClass(Text.class);
        //Reduce 갯수 설정
        summingJob.setNumReduceTasks(5);

        return summingJob.waitForCompletion(true) ? 0:1;

    }
    private boolean parseArgument(Job job1, Job job2, Job job3,Job job4,Job job5,Job job6, String[] args) throws IOException, org.apache.commons.cli.ParseException {
                            //(relevanceJob, scoringJob,UsercountJob,KeywordcountJob,InputJob, strings)
        Options options = getOptions();
        HelpFormatter helpFormatter = new HelpFormatter();
        if(args.length == 0){
            helpFormatter.printHelp("hadoop jar <JAR>" + getClass().getName(), options, true);
            return false;
        }


        // 커맨드 라인을 파싱한다.
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        //입력받은 파라메터가 정상적으로 입력되었는지 판단.
        for(String[] requirement : requirements){
            if(!cmd.hasOption(requirement[0])){ //input or output
                helpFormatter.printHelp("hadoop jar <JAR>" + getClass().getName(), options, true );
                return false;
            }
        }

        //여기까지 왔다는 것은 정상적으로 아규먼트가 입력되었다.
        if(cmd.hasOption("i")){
            FileInputFormat.addInputPaths(job1, cmd.getOptionValue("i"));
            FileInputFormat.addInputPaths(job3, cmd.getOptionValue("i"));
            FileInputFormat.addInputPaths(job4, cmd.getOptionValue("i"));
        }
        if(cmd.hasOption("o1")){
            FileOutputFormat.setOutputPath(job5, new Path(cmd.getOptionValue("o1")));
        }
        if(cmd.hasOption("o2")){
            FileOutputFormat.setOutputPath(job3, new Path(cmd.getOptionValue("o2")));
        }
        if(cmd.hasOption("o3")){
            FileOutputFormat.setOutputPath(job4, new Path(cmd.getOptionValue("o3")));
        }
        if(cmd.hasOption("o4")){
            FileOutputFormat.setOutputPath(job1, new Path(cmd.getOptionValue("o4")));
            FileInputFormat.addInputPaths(job2, cmd.getOptionValue("o4"));
        }
        if(cmd.hasOption("o5")){
            FileOutputFormat.setOutputPath(job2, new Path(cmd.getOptionValue("o5")));
            FileInputFormat.addInputPaths(job6, cmd.getOptionValue("o5"));
        }
        if(cmd.hasOption("o6")){
            FileOutputFormat.setOutputPath(job6, new Path(cmd.getOptionValue("o6")));
        }


        return true;
    }
}
