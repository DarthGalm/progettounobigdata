package JobThree;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class MeanByName {

    public static class MapperMean extends Mapper<LongWritable, Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] cols = value.toString().split(";");

            Text companyName = new Text();
            Text pricesData = new Text();
            //Text historicalStocksData = new Text();
            //il check potrebbe essere inutile dato che provendo da dati creati da me, puliti
            if (cols.length == 5) {
                companyName.set((cols[4]));
                pricesData.set(String.format("%s;%s;%s",cols[1], cols[2], cols[3]));
                context.write(companyName, pricesData);
            } else {
                System.out.println("qualcosa non va nella riga con ticker: " + cols[0]);
                return;
            }
        }
    }

    public static class ReducerMean extends Reducer<Text, Text, Text, Text> {

        //private TreeMap<Double, Text> sortMap = new TreeMap<Double, Text>(Collections.<Double>reverseOrder());

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String delim = ";";

            long sum2016 = 0, sum2017 = 0, sum2018 = 0;
            int recordCount = 0;

            for (Text val : values) { //formato è anno:valore per tutti gli anni
                String[] cols = StringUtils.getStrings(val.toString(), delim);
                //substring di 5 perchè tolgo il numero dell' anno, e rimuovo la "," per la convenzione italiana
                sum2016 += Long.parseLong(cols[0].substring(5)/*.replaceAll(",", ".")*/);
                sum2017 += Long.parseLong(cols[1].substring(5)/*.replaceAll(",", ".")*/);
                sum2018 += Long.parseLong(cols[2].substring(5)/*.replaceAll(",", ".")*/);
                recordCount++;
            }

            long avg2016 = 0,avg2017 = 0,avg2018 = 0;
            if(recordCount!=0) {
                avg2016 = sum2016 / recordCount;
                avg2017 = sum2017 / recordCount;
                avg2018 = sum2018 / recordCount;
            } else {
                System.out.println("errore: nessun valore nel record: " + key.toString());
                //skippo il record
                return;
            }

            if((avg2016 != 0) || (avg2017 != 0) || (avg2018 != 0)) {
                /*String yearAndVariation2016 = String.format("2016:%.2f", Double.isNaN(avg2016) ? 0 : avg2016);
                String yearAndVariation2017 = String.format("2017:%.2f", Double.isNaN(avg2017) ? 0 : avg2017);
                String yearAndVariation2018 = String.format("2018:%.2f", Double.isNaN(avg2018) ? 0 : avg2018);*/

                String yearAndVariation2016 = String.format("2016:%d", avg2016);
                String yearAndVariation2017 = String.format("2017:%d", avg2017);
                String yearAndVariation2018 = String.format("2018:%d", avg2018);

                String yearAndVariationValue = String.format("%s;%s;%s", yearAndVariation2016, yearAndVariation2017, yearAndVariation2018);

                context.write(key, new Text(yearAndVariationValue));
            } else {
                System.out.println("errore: le medie vengono tutte a 0 per valore: " + key.toString());
                return;
            }
        }
    }

}
