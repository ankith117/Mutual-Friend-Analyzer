import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q3MemoryJoin {


    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {

        Text user = new Text();
        Text friends = new Text();
        HashMap<String, String> state_map = new HashMap<>();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] line = value.toString().split("\\t");
            String user_id = line[0];
            if(line.length == 1) {
                return;
            }
            String[] friend_list = line[1].split(",");
            String user_pair;
            for(String friend : friend_list) {
                if(user_id.equals(friend))
                    continue;
                if(Integer.parseInt(user_id) < Integer.parseInt(friend)){
                    user_pair =  user_id + "," + friend;
                }
                else {
                    user_pair = friend + "," + user_id;
                }
                String new_line = line[1].replaceAll("\\b" + friend + ",", "");
                String[] new_friend_list = new_line.split(",");
                LinkedHashSet<String> friend_details = new LinkedHashSet<>();
                for ( String friend_id : new_friend_list){
                    if (!state_map.containsKey(friend_id))
                        return;
                    friend_details.add(friend_id + state_map.get(friend_id));
                }
                String new_line_2 = friend_details.toString().replaceAll("\\[|\\]", "").trim();
                if((new_line_2.length() != 0)) {
                    friends.set(new_line_2);
                }
                user.set(user_pair);
                context.write(user, friends);
            }
        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            //read data to memory on the mapper.
            Configuration conf = context.getConfiguration();

            Path part=new Path(context.getConfiguration().get("ARGUMENT"));//Location of file in HDFS


            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(part);
            for (FileStatus status : fss) {
                Path pt = status.getPath();
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line=br.readLine();
                while (line != null){
                    String[] arr=line.split(",");

                    state_map.put(arr[0], arr[1] + ": " + arr[5]);
                    line=br.readLine();
                }
            }
        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {

            String[] friend_lists = new String[2];
            int i = 0;
            for( Text value: values ) {
                friend_lists[i++] = value.toString();
            }
            if(friend_lists[0] == null || friend_lists[1] == null)
                return;
            String[] friends_list1 = friend_lists[0].split(",");
            String[] friends_list2 = friend_lists[1].split(",");
            LinkedHashSet<String> common_friends = new LinkedHashSet<>();
            for(String friend: friends_list1) {
                common_friends.add(friend.trim());
            }
            LinkedHashSet<String> common_friends_2 = new LinkedHashSet<>();
            for(String friend: friends_list2) {
                common_friends_2.add(friend.trim());
            }
            common_friends.retainAll(common_friends_2);
            String common_friends_list = common_friends.toString().replaceAll("\\[|\\]", "").replaceAll("[0-9]*", "");
            if((common_friends_list.length() != 0)) {
                common_friends_list = "[" + common_friends_list.trim() + "]";
                context.write(key, new Text(common_friends_list));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: Q3MemoryJoin <input> <input2> <output>");
            System.exit(2);
        }
        conf.set("ARGUMENT",otherArgs[1]);

        Job job = new Job(conf, "Q3MemoryJoin");
        job.setJarByClass(Q3MemoryJoin.class);
        job.setMapperClass(Q3MemoryJoin.Map.class);
        job.setReducerClass(Q3MemoryJoin.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}