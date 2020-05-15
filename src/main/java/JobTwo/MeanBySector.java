package JobTwo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Date;

//volume annuale medio, variazione annuale media e quotazione giornaliera media
class JobTwoResult
{
    private final String year;
    private long volumeSum;
    private double closeSum;
    private double percentVariationSum;
    private int avgCounter;

    private long volumeAvg;
    private Double closeAvg;
    private Double percentVariationAvg;

    public JobTwoResult(
            String year,
            long volumeSum,
            double percentVariationSum,
            double closeSum,
            int avgCounter
    )
    {
        this.year = year;
        this.volumeSum = volumeSum;
        this.closeSum = closeSum;
        this.percentVariationSum = percentVariationSum;
        this.avgCounter = avgCounter;
    }

    public String Year() {return year;}
    public int AvgCounter() { return avgCounter; }
    public long VolumeAvg() { return  volumeAvg; }
    public Double CloseAvg() { return closeAvg; }
    public Double PercentVariationAvg() { return percentVariationAvg; }

    public void updateVolumeSum(long volumeSum) { this.volumeSum += volumeSum; }
    public void updatePercentVariationSum(double percentVariationSum) { this.percentVariationSum += percentVariationSum; }
    public void updateCloseSum(double closeSum) { this.closeSum += closeSum; }
    public void updateAvgCounter() { this.avgCounter++;}

    public void calculateVolumeAvg(){
        volumeAvg = volumeSum / avgCounter;
    }
    public void calculateCloseAvg(){
        closeAvg = closeSum / avgCounter;
    }
    public void calculatePercentVariationAvg(){
        percentVariationAvg = percentVariationSum / avgCounter;
    }

}

public class MeanBySector {

    public static class MapperMean extends Mapper<LongWritable, Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] cols = value.toString().split(";");

            Text sector = new Text();
            Text stockData = new Text();
            //Text historicalStocksData = new Text();
            //il check potrebbe essere inutile dato che provendo da dati creati da me, puliti
            if (cols.length == 3) {
                sector.set((cols[2]));
                stockData.set((cols[1]));
                context.write(sector, stockData);
            } else {
                System.out.println("qualcosa non va nella riga con ticker: " + cols[0]);
            }
        }
    }

    public static class ReducerMean extends Reducer<Text, Text, Text, Text> {

        //private TreeMap<Double, Text> sortMap = new TreeMap<Double, Text>(Collections.<Double>reverseOrder());

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //array that stores every result for a sector from 2008 to 2018
            JobTwoResult[] results = new JobTwoResult[11];

            for (Text val : values) {
                //returns String[year:avgVolume:percentVariation:avgClose]
                String[] stockData = StringUtils.getStrings(val.toString(),",");

                for(int i = 0; i < stockData.length; i++){
                    String[] stockValues = StringUtils.getStrings(stockData[i],":");
                    String year = stockValues[0];
                    long volume = Long.parseLong(stockValues[1]);
                    double percentVariation = Double.parseDouble(stockValues[2]);
                    double close = Double.parseDouble(stockValues[3]);

                    if(results[i] == null && volume != 0 && percentVariation != 0.000 && close != 0.000)
                    {
                        results[i] = new JobTwoResult(year, volume, percentVariation, close, 1);
                    }
                    else if(volume != 0 && percentVariation != 0.000 && close != 0.000)
                    {
                        JobTwoResult result = results[i];
                        result.updateVolumeSum(volume);
                        result.updatePercentVariationSum(percentVariation);
                        result.updateCloseSum(close);
                        result.updateAvgCounter();
                    }
                }
            }

            String[] resultOutput = new String[11];
            boolean isOutputNull = true;

            //calculate the mean
            for  (int i = 0; i < results.length; i++) {
                JobTwoResult r = results[i];
                if(r != null) {
                    isOutputNull = false;
                    r.calculateVolumeAvg();
                    r.calculatePercentVariationAvg();
                    r.calculateCloseAvg();
                    String sign = r.PercentVariationAvg() > 0 ? "+" : "";
                    resultOutput[i] = String.format("{%s: [avgVolume: %d, avgPercent: %s%.3f%%, avgClose: %.3f]}",
                            r.Year(),r.VolumeAvg(),sign,r.PercentVariationAvg(),r.CloseAvg());
                }else {
                    resultOutput[i] = String.format("{%s: [avgVolume: %d, avgPercent: %.3f%%, avgClose: %.3f]}",i+2008,0,0.0,0.0);
                }
            }

            if(!isOutputNull) {
                String joinOutput = String.format("%s", String.join(",", resultOutput));
                context.write(key, new Text(joinOutput));
            }
        }
    }
}
