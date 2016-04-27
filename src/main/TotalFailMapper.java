import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.xml.bind.DatatypeConverter;

import java.security.MessageDigest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.ObjectOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.ObjectInputStream;
import org.apache.hadoop.fs.Path;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.*;

public class TotalFailMapper extends Mapper<LongWritable, Text, WTRKey,
                                            RequestReplyMatch> {

    private MessageDigest messageDigest;
    @Override
    public void setup(Context ctxt) throws IOException, InterruptedException {
        // You probably need to do the same setup here you did
        // with the QFD writer

        super.setup(ctxt);
        try {
            messageDigest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            // SHA-1 is required on all Java platforms, so this
            // should never occur
            throw new RuntimeException("SHA-1 algorithm not available");
        }
        // Now we are adding the salt...
        messageDigest.update(HashUtils.SALT.getBytes(StandardCharsets.UTF_8));

    }

    @Override
    public void map(LongWritable lineNo, Text line, Context ctxt)
            throws IOException, InterruptedException {

        // The value in the input for the key/value pair is a Tor IP.
        // You need to then query that IP's source QFD to get
        // all cookies from that IP,
        // query the cookie QFDs to get all associated requests
        // which are by those cookies, and store them in a torusers QFD


        //get the TorIP
        String torIP = line.toString();
        String hash = getHash(torIP);
        String filename = "qfds/srcIP/srcIP_" + hash;
        Path path = new Path(filename);
        
        FileSystem config = FileSystem.get(ctxt.getConfiguration());
        FSDataInputStream stream = config.open(path);
        ObjectInputStream in = new ObjectInputStream(stream);
        QueryFocusedDataSet qfd = null;
        try {
            qfd = (QueryFocusedDataSet) in.readObject();
            } catch (Exception e) {
            e.printStackTrace();
        }
        
        //query cookie 
        ArrayList<String> cookies = new ArrayList<String>();
        Set<RequestReplyMatch> matches = qfd.getMatches();
        for (RequestReplyMatch match : matches) {
            cookies.add(match.getCookie());
        }
        stream.close();
        in.close();

        for (String cookie : cookies) {
            //open the QFD associated with that cookie
            //System.out.println(cookie);
            hash = getHash(cookie);
            filename = "qfds/cookie/cookie_" + hash;
            path = new Path(filename);
            stream = config.open(path);
            in = new ObjectInputStream(stream);
            try {
                qfd = (QueryFocusedDataSet) in.readObject();
                } catch (Exception e) {
                e.printStackTrace();
            }
            matches = qfd.getMatches();
            for (RequestReplyMatch match : matches) {
                hash = getHash(match.getUserName());
                WTRKey key = new WTRKey("torusers", hash);
                ctxt.write(key, match);
            }
            stream.close();
            in.close();
        }
    }

    public String getHash(String attribute) {
        MessageDigest md = HashUtils.cloneMessageDigest(messageDigest);
        md.update(attribute.getBytes(StandardCharsets.UTF_8));
        byte[] hash = md.digest();
        byte[] hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
        String hashString = DatatypeConverter.printHexBinary(hashBytes);
        return hashString;
    } 
}
