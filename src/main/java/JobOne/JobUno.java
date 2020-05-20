package JobOne;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;


public class JobUno {

    static class Stock {
        //common
        public static int ticker = 0;

        //historical stock prices
        public int open = 1;
        public static int close = 2;
        public int adj_close = 3;
        public int lowThe = 4;
        public int highThe = 5;
        public static int volume = 6;
        public static int date = 7;

        //historical stocks
        public int exchange = 1;
        public int name = 2;
        public int secondName = 3;
        public int sector = 3;
        public int industry = 4;
    }

    public static class MapperUno extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value,
                           Context ctx) throws IOException, InterruptedException {
            String delim = ",";
            String[] cols = StringUtils.getStrings(value.toString(), delim);

            Text tickerSymbol = new Text();
            Text closePriceAndDate = new Text();

            if(cols.length == 8 && !(cols[0].equals("ticker"))) {
                if(this.isInDesiredRange(cols[Stock.date])) {
                    tickerSymbol.set((cols[Stock.ticker]));
                    closePriceAndDate.set(cols[Stock.close] + ";" + cols[Stock.date] + ";" +cols[Stock.volume]);
                    ctx.write(tickerSymbol, closePriceAndDate);
                }
            } else {
                System.out.println("trovato dato non completo" + cols[0]);
                return;
            }
        }
        //noi vogliamo solo dal 2008 al 2018
        private boolean isInDesiredRange(String date) {
            String referenceStartDate = "2008-01-01";
            SimpleDateFormat format = new SimpleDateFormat(
                    "yyyy-MM-dd");
            Date referenceDate = null;
            Date dateToCompare = null;
            try {
                referenceDate = format.parse(referenceStartDate);
                dateToCompare = format.parse(date);

            } catch (ParseException e) {
                System.out.println("errore parsing data");
                e.printStackTrace();
            }
            return dateToCompare.compareTo(referenceDate) > 0;
        }
    }

    public static class ReducerUno extends Reducer<Text, Text, Text, Text> {
        //per ordinamento
        private TreeMap<Double, Text> sortMap = new TreeMap<Double, Text>(Collections.<Double>reverseOrder());

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double maxPrice = 0, minPrice = 0;
            double firstPrice = 0, lastPrice = 0;
            double countRecords = 0;
            double sum = 0;
            double avg = 0;
            double percentVariation = 0;

            String delim = ";";

            Date minCheckedDate = null;
            Date maxCheckedDate = null;
            Date currentDate = null;

            // computes the number of occurrences of a single word
            for (Text val : values) {
                String[] cols = StringUtils.getStrings(val.toString(), delim);
                double price = Double.parseDouble(cols[0]);
                double volume = Double.parseDouble(cols[2]);
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    //inizializzo
                    currentDate = format.parse(cols[1]);
                } catch (ParseException e) {
                    System.out.println("Errore parsing data due");
                    e.printStackTrace();
                }
                //se sono uguali a 0 e' la prima passata
                if (firstPrice == 0 && lastPrice == 0) {
                    firstPrice = price;
                    lastPrice = price;
                    maxPrice = price;
                    minPrice = price;
                    sum = volume;
                    countRecords = 1;
                    minCheckedDate = currentDate;
                    maxCheckedDate = currentDate;
                } else { //non e' la prima passata

                    if (price > maxPrice) {
                        maxPrice = price;
                    } else if (price < minPrice) {
                        minPrice = price;
                    }

                    if (currentDate.compareTo(maxCheckedDate) > 0) {
                        lastPrice = price;
                        maxCheckedDate = currentDate;
                    } else if (currentDate.compareTo(minCheckedDate) < 0) {
                        firstPrice = price;
                        minCheckedDate = currentDate;
                    }

                    sum += volume;
                    countRecords++;
                }
            }
            avg = sum / countRecords;
            percentVariation = ((lastPrice - firstPrice) / firstPrice) * 100;

            String ticker = key.toString();
            String percentVar = String.format("percent variation: %.2f ", percentVariation);
            String minAndMax = String.format("min: %.2f max: %.2f ", minPrice, maxPrice);
            String avgVolume = String.format("avg: %.2f", avg);

            String textToWrite = ticker + ";" + percentVar + minAndMax + avgVolume;

            sortMap.put(percentVariation, new Text(textToWrite));

//            for (Text sortMapText : sortMap.values()) {
//                context.write(key, sortMapText);
//            }

//            context.write(key, new Text(textToWrite));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String delim = ";";
            for (Text sortMapText : sortMap.values()) {
                String[] cols = StringUtils.getStrings(sortMapText.toString(), delim);
                context.write(new Text(cols[0]), new Text(cols[1]));
            }
        }
    }

    //main contenuto in una classe sola
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job jobUno = Job.getInstance(conf, "JobOne.JobUno");
        jobUno.setJarByClass(JobUno.class);
        jobUno.setMapperClass(MapperUno.class);
        jobUno.setReducerClass(ReducerUno.class);
        jobUno.setInputFormatClass(TextInputFormat.class);
        jobUno.setMapOutputKeyClass(Text.class);
        jobUno.setMapOutputValueClass(Text.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileInputFormat.addInputPath(jobUno, input);
        FileOutputFormat.setOutputPath(jobUno, output);

        boolean succ = jobUno.waitForCompletion(true);
        if (!succ) {
            System.out.println("Job1 failed, exiting");
        }
        return;
    }
}
