/**
 * Created by felicien on 10/10/16.
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class CountFirstNameByOrigin {

    private static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            //Récupère la line à traiter
            String[] lineSplit = line.split(";");
            //Sépare la ligne selon les ;
            String[] originSplit = lineSplit[2].split(",");
            //Sépare les origines avec les virgules
            for (String s : originSplit) {
                //Les prénoms avec pour origine "" ou "?" sont considérés comme "_unknown"
                if (s.equals("") || s.equals("?")) {
                    s = "_unknown";
                }
                word.set(s.trim());
                output.collect(word, one);
                //Ajout d'un 1 dans le dictionnaire pour le mot correspondant
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                //Compte le nombre de 1 de la value
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(CountFirstNameByOrigin.class);
        conf.setJobName("countFirstNameByOrigin");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
