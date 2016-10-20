import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by felicien on 19/10/16.
 */
public class ProportionMaleFemale {

    private static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
        private final static FloatWritable one = new FloatWritable(1);
        private final static FloatWritable zero = new FloatWritable(0);
        private Text word = new Text();
        private Text f = new Text("f");
        private Text m = new Text("m");
        public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            //Récupère la line à traiter
            String[] lineSplit = line.split(";");
            //Sépare la ligne selon les ;
            String[] originSplit = lineSplit[1].split(",");
            //Sépare les origines avec les virgules
            for (String s : originSplit) {
                word.set(s.trim());

                /*
                    L'objectif ici est de remplir normalement l'ouputCollector, mais également de mettre un 0 dans
                    la case non correspondante.
                    Cela permet d'avoir le nombre total de personne dans chacune des outputs.
                    On pourra ensuite calculer le pourcentage dans le reducer
                 */
                if (word.equals(f)){
                    output.collect(f, one);
                    output.collect(m, zero);
                }
                else{
                    output.collect(f, zero);
                    output.collect(m, one);
                }

                //Ajout d'un 1 dans le dictionnaire pour le mot correspondant
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            float sum = 0;
            float total = 0;
            float percent;
            while (values.hasNext()) {
                //Compte le nombre de 1 de la value
                sum += values.next().get();
                total++;
            }

            //Le pourcentage est obtenu grace au nombre total de personne et du nombre de values.
            percent = sum/total*10;
            output.collect(key, new FloatWritable(percent));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(ProportionMaleFemale.class);
        conf.setJobName("ProportionMaleFemale");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(FloatWritable.class);

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

