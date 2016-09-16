import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by user on 14. 5. 2.
 */
public class FileWriterTest {
    public static void main(String[] args) throws IOException {
        String filePath = "./";
        int count = 0;
        FileWriter fw = new FileWriter(filePath + "\\output" + count);
        String[] tweetList = {
                "aa", "bb", "cc"
        };

        StringBuilder writeData = new StringBuilder();
        for (String tweet : tweetList) {
            writeData.append(tweet+"\n");
        }
        fw.write(writeData.toString());
        fw.close();

    }
}