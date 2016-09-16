package com.sunghwan.kim;

/**
 * Created by user on 14. 3. 16.
 */

import twitter4j.*;

import java.io.File;
import java.io.IOException;
public final class FilterStreamTweets {
    public static void main(String[] args) throws TwitterException, IOException {
        if (args.length == 0) {
            System.err.println("파일 저장 경로를 설정하세요");
            System.exit(1);
        }
        File fileFolder = new File(args[0]);
        try{
            if(!fileFolder.exists()){
                fileFolder.mkdir();
            }
        }catch (Exception e){
            System.out.println("파일 경로를 설정할수 없습니다");
        }

        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();

        Listener listenerClass = new Listener(args[0]);

        twitterStream.addListener(listenerClass); //리스너 추가

        twitterStream.sample();
    }
}