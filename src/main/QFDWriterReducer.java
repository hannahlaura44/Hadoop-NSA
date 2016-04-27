import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.ObjectOutputStream;
import org.apache.hadoop.fs.Path;
import java.util.HashSet;

import java.io.IOException;

public class QFDWriterReducer extends Reducer<WTRKey, RequestReplyMatch, NullWritable, NullWritable> {

    @Override
    public void reduce(WTRKey key, Iterable<RequestReplyMatch> values,
                       Context ctxt) throws IOException, InterruptedException {

        //open file and write all the values into it.

        // The input will be a WTR key and a set of matches.

        // You will want to open the file named
        // "qfds/key.getName()/key.getName()_key.getHashBytes()"
        // using the FileSystem interface for Hadoop.

        // EG, if the key's name is srcIP and the hash is 2BBB,
        // the filename should be qfds/srcIP/srcIP_2BBB

        // Some useful functionality:

        // FileSystem.get(ctxt.getConfiguration())
        // gets the interface to the filesysstem
        // new Path(filename) gives a path specification
        // hdfs.create(path, true) will create an
        // output stream pointing to that file
        HashSet<RequestReplyMatch> set = new HashSet<RequestReplyMatch>();
        RequestReplyMatch rrmlocal = new RequestReplyMatch();
        for (RequestReplyMatch val : values) {
            rrmlocal = new RequestReplyMatch(val);
            set.add(rrmlocal);
        }
        QueryFocusedDataSet qfd = new QueryFocusedDataSet(key.getName(), key.getHashBytes(), set);

        FileSystem config = FileSystem.get(ctxt.getConfiguration());
        String filename = "qfds/" + key.getName() + "/" + key.getName() + "_" + key.getHashBytes();
        Path path = new Path(filename);
        FSDataOutputStream stream = config.create(path, true);
        ObjectOutputStream out = new ObjectOutputStream(stream);
        out.writeObject(qfd);
        //close objectoutputstream and FSdataoutputstream
        stream.close();
        out.close();
    }
}
