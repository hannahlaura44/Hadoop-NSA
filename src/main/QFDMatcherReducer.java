import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import java.util.ArrayList;

import java.io.IOException;

public class QFDMatcherReducer extends Reducer<IntWritable, WebTrafficRecord, RequestReplyMatch, NullWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<WebTrafficRecord> values,
                       Context ctxt) throws IOException, InterruptedException {

        // The input is a set of WebTrafficRecords for each key,
        // the output should be the WebTrafficRecords for
        // all cases where the request and reply are matched
        // as having the same
        // Source IP/Source Port/Destination IP/Destination Port
        // and have occured within a 10 second window on the timestamp.

        // One thing to really remember, the Iterable element passed
        // from hadoop are designed as READ ONCE data, you will
        // probably want to copy that to some other data structure if
        // you want to iterate mutliple times over the data.

        //make new data struct arraylist
        ArrayList<WebTrafficRecord> records = new ArrayList();
        WebTrafficRecord localRecord = new WebTrafficRecord();
        for (WebTrafficRecord val : values) {
            localRecord = new WebTrafficRecord(val);
            records.add(localRecord);
        }

        
        //there's only one match!
        //theres multiple requests and multiple replies
        //nested for loops. outer for loop could check for requests and inner for loop checks for reply.
        //can assume there is only one. emit the requestreplymatch 
        long timeDiff;
        for (WebTrafficRecord record : records) {
            if (record.getUserName() != null) {
                //this is a reply
                for (WebTrafficRecord record2 : records) {
                    timeDiff = Math.abs(record.getTimestamp() - record2.getTimestamp());
                    if ((record2.getCookie() != null) & (timeDiff <= 10)) {
                        //this is a request
                        RequestReplyMatch match = new RequestReplyMatch(record2, record);
                        ctxt.write(match, NullWritable.get());
                    }
                }
            } else {
                //this is a request
                for (WebTrafficRecord record2 : records) {
                    timeDiff = Math.abs(record.getTimestamp() - record2.getTimestamp());
                    if ((record2.getUserName() != null) & (timeDiff <= 10)) {
                        //this is a reply
                        RequestReplyMatch match = new RequestReplyMatch(record, record2);
                        ctxt.write(match, NullWritable.get());
                    }
                }
            }
        }


        // ctxt.write should be RequestReplyMatch and a NullWriteable
    }
}
