package JobThree;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import Objects.Stock;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import static java.util.Calendar.*;

/* Fatta da Filippo Frillici
    questa classe prende in input i due dataset in csv
 */

public class JoinMR {
    //metodi di supporto
    public static boolean isInDesiredRange(String date) {
        String referenceStartDate = "2016-01-01";
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
                    historicalStockPricesData.set(String.format("%s,%s,%s", "hsp",cols[Stock.close], cols[Stock.date]));
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
            //String[] cols = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); //trovata su Stack Overflow
            String[] cols = value.toString().split(",");
            Text tickerSymbol = new Text();
            //Text historicalStockPricesData = new Text();
            Text historicalStocksData = new Text();
            int colsLength = cols.length;
            //elimino la prima riga esplicativa, dati incompleti, e i ticker i cui nomi contengono un apice, essendo sporchi
            if ( (colsLength >= 5) && !(cols[0].equals("ticker")) && !(cols[Stock.ticker].contains("\\^")) ) {
                if(cols[Stock.ticker].contains("^")) { //non mi funziona nel precedente if
                    System.out.println("ticker contiene apice: " + cols[Stock.ticker]);
                    return;
                }
                //alcune hanno una virgola nel nome azienda, prima di Inc. ed elaboro quel caso
                String secondPart = "";
                secondPart = colsLength > 5 ? cols[Stock.secondName] : "";
                tickerSymbol.set(cols[Stock.ticker]);
                historicalStocksData.set(String.format("%s,%s%s", "hs",cols[Stock.name], secondPart));
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
            double max2016 = 0; //uso min e max al posto di first e last per sovrapposizione
            double max2017 = 0; //di nomi con variabile di tipo Date
            double max2018 = 0;
            double min2016 = 0;
            double min2017 = 0;
            double min2018 = 0;
            //Stringhe di supporto
            String delim = ",";
            //String ticker = key.toString(); usato all' inizio perche' dava problemi
            String yearAndVariationValue = "";
            String nameValue = "";
            //Date di supporto
            Date first2016 = null;
            Date first2017 = null;
            Date first2018 = null;

            //ultima data utile di ogni anno
            Date last2016 = null;
            Date last2017 = null;
            Date last2018 = null;

            Date dateToCompare = null; //la data di riferimento ad ogni passata

            //altro
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Calendar calendar = getInstance();

            for (Text val : values) {
                String[] cols = StringUtils.getStrings(val.toString(), delim);
                if(cols[0].equals("hsp")){ //sto elaborando dati da historical stock prices
                    double price = Double.parseDouble(cols[1]);
                    try {
                        dateToCompare = format.parse(cols[2]);
                        calendar.setTime(dateToCompare);
                    } catch (ParseException e) {
                        System.out.println("Linea corrotta: ticker " + key.toString());
                        e.printStackTrace();
                    }
                    int year = calendar.get(YEAR);
                    if(year == 2016) {
                        if(min2016==0 && max2016==0) { //prima passata
                            min2016 = price;
                            max2016 = price;
                            first2016 = dateToCompare;
                            last2016 = dateToCompare;
                        } else {
                            if (dateToCompare.compareTo(first2016) < 0) {
                                min2016 = price;
                                first2016 = dateToCompare;
                            } else if (dateToCompare.compareTo(last2016) > 0) {
                                max2016 = price;
                                last2016 = dateToCompare;
                            }
                        }
                    } else if(year == 2017) {
                        if(min2017==0 && max2017==0) { //prima passata
                            min2017 = price;
                            max2017 = price;
                            first2017 = dateToCompare;
                            last2017 = dateToCompare;
                        } else {
                            if (dateToCompare.compareTo(first2017) < 0) {
                                min2017 = price;
                                first2017 = dateToCompare;
                            } else if (dateToCompare.compareTo(last2017) > 0) {
                                max2017 = price;
                                last2017 = dateToCompare;
                            }
                        }
                    } else if(year == 2018) {
                        if(min2018==0 && max2018==0) { //prima passata
                            min2018 = price;
                            max2018 = price;
                            first2018 = dateToCompare;
                            last2018 = dateToCompare;
                        } else {
                            if (dateToCompare.compareTo(first2018) < 0) {
                                min2018 = price;
                                first2018 = dateToCompare;
                            } else if (dateToCompare.compareTo(last2018) > 0) {
                                max2018 = price;
                                last2018 = dateToCompare;
                            }
                        }
                    } else {
                        System.out.println("Errore di parsing dell' anno per ticker: " + key.toString());
                    }
                    yearAndVariationValue = String.format("%s,%s", cols[1], cols[2]);
                } else if(cols[0].equals("hs")) { //sto elaborando dati da historical stocks
                    nameValue = cols[1];
                }
            }
            //con min e max intendo il primo e l' ultimo, come per il job 1
            Double percentVariation2016 = (((max2016 - min2016) / min2016) * 100);
            Double percentVariation2017 = ((max2017 - min2017) / min2017) * 100;
            Double percentVariation2018 = ((max2018 - min2018) / min2018) * 100;
            percentVariation2016 = Double.isNaN(percentVariation2016) ? 0 : percentVariation2016;
            percentVariation2017 = Double.isNaN(percentVariation2017) ? 0 : percentVariation2017;
            percentVariation2018 = Double.isNaN(percentVariation2018) ? 0 : percentVariation2018;
            String yearAndVariation2016 = String.format("%s:%d","2016",percentVariation2016.longValue());
            String yearAndVariation2017 = String.format("%s:%d","2017",percentVariation2017.longValue());
            String yearAndVariation2018 = String.format("%s:%d","2018",percentVariation2018.longValue());
            yearAndVariationValue = String.format("%s;%s;%s", yearAndVariation2016,yearAndVariation2017,yearAndVariation2018);

            String joinOutput = String.format("%s;%s",yearAndVariationValue, nameValue);
            context.write(key, new Text(joinOutput));
        }
    }
}
