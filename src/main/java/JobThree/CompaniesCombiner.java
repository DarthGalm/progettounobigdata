package JobThree;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class CompaniesCombiner {

    public static class MapperCombiner extends Mapper<LongWritable, Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Dato che ho un input già ben formato, uso solo i values del job precedente come chiave e viceversa
        // dopo averli ricondotti all' intero più vicino per trovare dei risultati
            String[] cols = value.toString().split(";");
            Text pricesYearly = new Text();
            Text companyName = new Text();
            //un ultimo check, potrebbero esserci dati sporchi anche se poco probabile
            if (cols.length == 4) {
                companyName.set((cols[0]));
                //arrotondamento dei numeri all' intero più vicino per far uscire i dati

                long price2016 = Long.parseLong(cols[1].substring(5));
                long price2017 = Long.parseLong(cols[2].substring(5));
                long price2018 = Long.parseLong(cols[3].substring(5));

                //non serve specificare l' anno visto che vengono messi in ordine 2016-2017-2018
                pricesYearly.set(String.format("%d;%d;%d",price2016, price2017, price2018));
                context.write(pricesYearly, companyName);
            } else {
                System.out.println("qualcosa non va nella riga della compagnia: " + cols[0]);
                return;
            }
        }
    }

    public static class ReducerCombiner extends Reducer<Text, Text, Text, Text> {

        //private TreeMap<Double, Text> sortMap = new TreeMap<Double, Text>(Collections.<Double>reverseOrder());

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String delim = ";";
            String result = "{";

            int count = 0;

            for (Text val : values) {
                result = result.concat(val.toString() + ", ");
                count++;
            }
           if(count < 2) {
               return;
            }

            result = result.substring(0,result.length()-2); //cut degli ultimi 2 caratteri ovvero virgola e whitespace
            result = result.concat("}: ");

            String[] cols = StringUtils.getStrings(key.toString(), delim);

            if(cols.length == 3) {
                String yearAndVariation2016 = String.format("2016:%s%%", cols[0]);
                String yearAndVariation2017 = String.format("2017:%s%%", cols[1]);
                String yearAndVariation2018 = String.format("2018:%s%%", cols[2]);

                String yearAndVariationValue = String.format("%s, %s, %s", yearAndVariation2016, yearAndVariation2017, yearAndVariation2018);
                context.write(new Text(result.replaceAll("\"", "")), new Text(yearAndVariationValue));
            } else {
                System.out.println("errore: controllare riga con prezzi " + key.toString());
                return;
            }
        }
    }

}
