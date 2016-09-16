package com.sunghwan.kim;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Created by user on 14. 4. 10.
 */
public class Listener implements StatusListener {
    static final int maxCount = 30;
    String filePath;
    FileWriter fw; //저장파일
    BufferedWriter bw;
    int count;
    int outputNumber;
    String[] tweet = new String[maxCount];
    Date date;

    public Listener(String filePath) throws IOException {
        this.filePath = filePath;
        count = 0;
        outputNumber = 0;
    }

    @Override
    public void onStatus(Status status) {
        if (status.getLang().equals("ko")) {
            try {
                date = new Date();
                tweet[count] = status.getId() + "\t" + status.getUser().getScreenName() + "\t" + "\"" + (new Timestamp(date.getTime())) + "\"" + "\t" + status.getText().replace("\t", " ").replace("\n", " ");
                count++;
                if (count >= maxCount) {
                    fw = new FileWriter(filePath + "/output" + outputNumber);
                    bw = new BufferedWriter(fw);
                    for (int i = 0; i < maxCount; i++) {
                        bw.write(tweet[i]);
                        bw.newLine();
                    }
                    bw.close();
                    System.out.println("create file : "+filePath+"/output"+outputNumber);
                    outputNumber++;
                    count = 0;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
//        System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
//        System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
//        System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
    }

    @Override
    public void onStallWarning(StallWarning warning) {
//        System.out.println("Got stall warning:" + warning);
    }

    @Override
    public void onException(Exception ex) {
        ex.printStackTrace();
    }

}
