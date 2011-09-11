import com.mongodb.*;
import com.mongodb.util.JSON;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.StatusListener;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.internal.org.json.JSONObject;
import twitter4j.json.DataObjectFactory;



import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: fhsu
 * Date: 9/8/11
 * Time: 5:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class TwitterDump {
    public static void main(String[] args) throws TwitterException, IOException {
        Mongo m = new Mongo("localhost");
        DB db = m.getDB("twitterdb");
        int tweetCount = args[0].length() > 0 ? Integer.parseInt(args[0]) : 100;

        final DBCollection coll = db.getCollection("test");

        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                //System.out.println(status.getUser().getName() + " : " + status.getText());
                String tweet = DataObjectFactory.getRawJSON(status);
                //System.out.println(tweet);
                DBObject doc = (DBObject)JSON.parse(tweet);
                coll.insert(doc);

            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            public void onScrubGeo(long l, long l1) {
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);
        twitterStream.sample();
        while (coll.count() < tweetCount) {

        }
        twitterStream.shutdown();
/*
        DBCursor cursorDoc = coll.find();

        while (cursorDoc.hasNext()) {
            System.out.println(cursorDoc.next());
        }
        */
    }
}
