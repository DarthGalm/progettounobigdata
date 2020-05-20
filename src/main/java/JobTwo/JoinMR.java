package JobTwo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import Objects.Stock;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import static java.util.Calendar.*;

class JobTwoStock
{
    private final String year;
    //per calcolare la variazione percentuale da firstDate a lastDate
    private double lastClose;
    private double firstClose;
    private Date lastDate;
    private Date firstDate;
    private Double percentVariation;
    //per calcolare il volume e quotazione media
    private long volumeSum;
    private double closeSum;
    private int avgCounter;
    private long volumeAvg;
    private Double closeAvg;

    public JobTwoStock(
            String year,
            double firstClose,
            double lastClose,
            Date firstDate,
            Date lastDate,
            long volumeSum,
            double closeSum,
            int avgCounter
    )
    {
        this.year = year;
        this.lastClose = lastClose;
        this.firstClose = firstClose;
        this.lastDate = lastDate;
        this.firstDate = firstDate;
        this.volumeSum = volumeSum;
        this.closeSum = closeSum;
        this.avgCounter = avgCounter;
    }

    public String Year() {return year;}
    public double LastClose()   { return lastClose; }
    public double FirstClose() { return firstClose; }
    public Date LastDate()   { return lastDate; }
    public Date FirstDate() { return firstDate; }
    public Double PercentVariation() { return percentVariation; }
    public long VolumeSum() { return volumeSum; }
    public double CloseSum() { return closeSum; }
    public int AvgCounter() { return avgCounter; }
    public long VolumeAvg() { return  volumeAvg; }
    public Double CloseAvg() { return closeAvg; }

    public void setLastDate(Date lastDate) { this.lastDate = lastDate; }
    public void setFirstDate(Date firstDate) { this.firstDate = firstDate; }
    public void setLastClose(double lastClose) { this.lastClose = lastClose; }
    public void setFirstClose(double firstClose) { this.firstClose = firstClose; }
    public void setVolumeSum(long volumeSum) { this.volumeSum = volumeSum; }
    public void setCloseSum(double closeSum) { this.closeSum = closeSum; }
    public void setAvgCounter(int avgCounter) { this.avgCounter = avgCounter;}

   public void calculatePercentVariation() {
        percentVariation = (((lastClose - firstClose) / firstClose) * 100);
    }
    /*
    public void calculateVolumeAvg(){
        volumeAvg = volumeSum / avgCounter;
    }
    public void calculateCloseAvg(){
        closeAvg = closeSum / avgCounter;
    }*/

}

/* Fatta da Filippo Frillici e Danilo Costa
    questa classe prende in input i due dataset in csv
 */
public class JoinMR {
    //metodi di supporto
    public static boolean isInDesiredRange(String date) {
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

    public static class MapperHistoricalStockPrices extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] cols = value.toString().split(",");

            Text tickerSymbol = new Text();
            Text historicalStockPricesData = new Text();
            //Text historicalStocksData = new Text();

            if (cols.length == 8 && !(cols[0].equals("ticker"))) {
                if (JoinMR.isInDesiredRange(cols[Stock.date])) {
                    tickerSymbol.set((cols[Stock.ticker]));
                    historicalStockPricesData.set(String.format("%s,%s,%s,%s", "hsp",cols[Stock.close], cols[Stock.date], cols[Stock.volume]));
                    context.write(tickerSymbol, historicalStockPricesData);
                }
            } else {
                System.out.println("trovato dato non completo: " + cols[0]);
                return;
            }
        }
    }

    public static class MapperHistoricalStocks extends Mapper<LongWritable,Text,Text,Text> {
        private int count;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //delim ","
            String[] cols = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); //trovata su Stack Overflow
            //String[] cols = value.toString().split(",");
            Text tickerSymbol = new Text();
            //Text historicalStockPricesData = new Text();
            Text historicalStocksData = new Text();
            int colsLength = cols.length;
            //elimino la prima riga esplicativa, dati incompleti, e i ticker i cui nomi contengono un apice, essendo sporchi
            if ( (colsLength >= 5) && !(cols[0].equals("ticker")) && !(cols[Stock.ticker].contains("^")) ) {
                if(cols[Stock.ticker].contains("^")) { //non mi funziona nel precedente if
                    System.out.println("ticker contiene apice: " + cols[Stock.ticker]);
                    return;
                }
                //alcune hanno una virgola nel nome azienda, prima di Inc. ed elaboro quel caso
               /* String sector = colsLength > 5 ? cols[cols.length - 2] : cols[Stock.sector];*/
                String sector = cols[Stock.sector];
                tickerSymbol.set(cols[Stock.ticker]);
                historicalStocksData.set(String.format("%s,%s", "hs",sector));
                //System.out.println("ticker contiene apice ma comunque è in context write: " + cols[Objects.Stock.ticker]);
                context.write(tickerSymbol, historicalStocksData);
            } else {
                System.out.println("trovato dato non completo: " + cols[0]);
                return;
            }
        }
    }

    public static class ReducerJoin extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //numeri di supporto
            //array che conserva la prima close e ultima close e le loro rispettive date degli anni da 2008 a 2018
            //anno 2008 in posizione 0 dell'array
            //anno 2018 in posizione 10
            JobTwoStock[] jobTwoStocks = new JobTwoStock[11]; //


            //Stringhe di supporto
            String delim = ",";
            //String ticker = key.toString(); usato all' inizio perche' dava problemi
            String yearAndVariationValue = "";
            String nameValue = "";

            Date dateToCompare = null; //la data di riferimento ad ogni passata

            //altro
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Calendar calendar = getInstance();

            for (Text val : values) {
                String[] cols = StringUtils.getStrings(val.toString(), delim);
                if(cols[0].equals("hsp")){ //sto elaborando dati da historical stock prices
                    double close = Double.parseDouble(cols[1]);
                    long volume = Long.parseLong(cols[3]);

                    try {
                        dateToCompare = format.parse(cols[2]);
                        calendar.setTime(dateToCompare);
                    } catch (ParseException e) {
                        System.out.println("Linea corrotta: ticker " + key.toString());
                        e.printStackTrace();
                    }
                    int year = calendar.get(YEAR);

                    int index = year - 2008;
                    JobTwoStock toModify = jobTwoStocks[index];

                    if(toModify == null) { //prima passata
                        jobTwoStocks[index] = new JobTwoStock(String.valueOf(year), close, close, dateToCompare, dateToCompare, volume, close, 1);
                    } else if(dateToCompare != null) {
                        if (dateToCompare.compareTo(toModify.FirstDate()) < 0) {
                            toModify.setFirstClose(close);
                            toModify.setFirstDate(dateToCompare);
                        } else if (dateToCompare.compareTo(toModify.LastDate()) > 0) {
                            toModify.setLastClose(close);
                            toModify.setLastDate(dateToCompare);
                        }
                        toModify.setVolumeSum(toModify.VolumeSum() + volume);
                        toModify.setCloseSum(toModify.CloseSum() + close);
                        toModify.setAvgCounter(toModify.AvgCounter() + 1);
                    }else {
                        System.out.println("Errore di parsing dell' anno per ticker: " + key.toString());
                    }
                } else if(cols[0].equals("hs")) { //sto elaborando dati da historical stocks
                    nameValue = cols[1];
                }
            }

            String[] stockOutput = new String[11];
            boolean isOutputNull = true;

            //variazioni percentuali, volume medio e quotazione media da 2008 and 2018
            for (int i = 0; i < jobTwoStocks.length; i++)
            {
                JobTwoStock o = jobTwoStocks[i];
                if(o != null) {
                    isOutputNull = false;
                    o.calculatePercentVariation();
                    /*
                    o.calculateCloseAvg();
                    o.calculateVolumeAvg();
                    */
                    stockOutput[i] = String.format("%s:%d:%.3f:%.3f:%d",o.Year(),o.VolumeSum(),o.PercentVariation(),o.CloseSum(),o.AvgCounter());
                }else{
                    stockOutput[i] = String.format("%s:%d:%.3f:%.3f:%d",i+2008,0,0.0,0.0,0);
                }

            }

            if(!isOutputNull) {
                String joinOutput = String.format("%s;%s", String.join(",", stockOutput), nameValue);
                context.write(key, new Text(joinOutput));
            }
        }
    }
}
