import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Main {

    static final String DATE_FORMAT = "yyyy-MM-dd";
    static final String url = "jdbc:postgresql://127.0.0.1:5432/test_db";
    static final String name = "postgres";
    static final String password = "12345";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // DB init
        Connection connection = initConection(url, name, password);

        final DateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
        // Spark init
        SparkConf conf = new SparkConf().setAppName("jb_etl_test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // reading required csv files
        JavaRDD<String> cityData = sc.textFile("GlobalLandTemperaturesByCity.csv");
        JavaRDD<String> countryData = sc.textFile("GlobalLandTemperaturesByCountry.csv");
        // excluding headers before processing
        cityData = prepareData(cityData);
        countryData = prepareData(countryData);

        // parsing lines of both files into objects of defined classes CountryRecord and CityRecord
        JavaRDD<CityRecord> cityD = cityData.map((Function<String, CityRecord>) line -> {
            String[] fields = line.split(",");
            Timestamp ts = null;
            try {
                Date dt = formatter.parse(fields[0]);
                ts = new Timestamp(dt.getTime());
            } catch (ParseException e) {
                System.out.println("ParseException!");
            }
            return new CityRecord(ts, fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]);
        });

        JavaRDD<CountryRecord> countryD = countryData.map((Function<String, CountryRecord>) line -> {
            String[] fields = line.split(",");
            Timestamp ts = null;
            try {
                Date dt = formatter.parse(fields[0]);
                ts = new Timestamp(dt.getTime());
            } catch (ParseException e) {
                System.out.println("ParseException!");
            }
            return new CountryRecord(ts, fields[1], fields[2], fields[3]);
        });

        // Average, max and min temperatures for each city per year, century and for the whole period.
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> cRecordsByYear = cityD.mapToPair(cityRecord ->
                new Tuple2<>(new Tuple2<>(cityRecord.getCity(), year(cityRecord.getDt())), new Tuple2<> (cityRecord.getAvgTemp(), 1)));
        // Excluding rows with null average temperature as they will not influence on statistics (but will fail with NPE)
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> notNullCrecsByYear =
                cRecordsByYear.filter((Function<Tuple2<Tuple2<String, Integer>, Tuple2<Double, Integer>>, Boolean>) obj
                        -> obj._2._1 != null);
        // Here key consists of two parts: city and year. By this key we reduce in order to count items, find average, max and min temperatures
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> cReducedByYear = notNullCrecsByYear.reduceByKey((tuple1,tuple2) ->
                new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> cAverageByYear = cReducedByYear.mapToPair(getAverageByPeriod);
        // Database insertion
        cAverageByYear.foreach((item) -> {
            String city = item._1._1;
            Integer year = item._1._2;
            Double avgTemp = item._2._1;
            String request = "INSERT INTO cities_year (city, year, avgtemp) VALUES (?, ?, ?);";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setString(1, city);
            insertion.setInt(2, year);
            insertion.setDouble(3, avgTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> cMaxaggByYear = notNullCrecsByYear.reduceByKey(Main::calcMax);
        // Inserting max value per city into additional column of the same table with items stored per year
        cMaxaggByYear.foreach((item) -> {
            String city = item._1._1;
            Integer year = item._1._2;
            Double maxTemp = item._2._1;
            String request = "INSERT INTO cities_year (maxTemp) VALUES (?) WHERE city='"+city+"' AND year='"+year+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, maxTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> cMinaggByYear = notNullCrecsByYear.reduceByKey(Main::calcMin);
        // Inserting min value per city into additional column of the same table with items stored per year
        cMinaggByYear.foreach((item) -> {
            String city = item._1._1;
            Integer year = item._1._2;
            Double minTemp = item._2._1;
            String request = "INSERT INTO cities_year (minTemp) VALUES (?) WHERE city='"+city+"' AND year='"+year+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, minTemp);
            insertion.executeUpdate();
        });

        // Processing of rows and database insertion with items per century is performed in a same way as per year
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> cRecordsByCentury = cityD.mapToPair(cityRecord ->
                new Tuple2<>(new Tuple2<>(cityRecord.getCity(), century(cityRecord.getDt())), new Tuple2<> (cityRecord.getAvgTemp(), 1)));

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> notNullCrecsByCentury =
                cRecordsByCentury.filter((Function<Tuple2<Tuple2<String, Integer>, Tuple2<Double, Integer>>, Boolean>) obj
                        -> obj._2._1 != null);

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> cReducedByCentury = notNullCrecsByCentury.reduceByKey((tuple1,tuple2) ->
                new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> cAverageByCentury = cReducedByCentury.mapToPair(getAverageByPeriod);
        cAverageByCentury.foreach((item) -> {
            String city = item._1._1;
            Integer century = item._1._2;
            Double avgTemp = item._2._1;
            String request = "INSERT INTO cities_century (city, century, avgtemp) VALUES (?, ?, ?);";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setString(1, city);
            insertion.setInt(2, century);
            insertion.setDouble(3, avgTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> cMaxaggByCentury = notNullCrecsByCentury.reduceByKey(Main::calcMax);
        cMaxaggByCentury.foreach((item) -> {
            String city = item._1._1;
            Integer century = item._1._2;
            Double maxTemp = item._2._1;
            String request = "INSERT INTO cities_century (maxTemp) VALUES (?) WHERE city='"+city+"' AND century='"+century+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, maxTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> cMinaggByCentury = notNullCrecsByCentury.reduceByKey(Main::calcMin);
        cMinaggByCentury.foreach((item) -> {
            String city = item._1._1;
            Integer century = item._1._2;
            Double minTemp = item._2._1;
            String request = "INSERT INTO cities_century (minTemp) VALUES (?) WHERE city='"+city+"' AND century='"+century+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, minTemp);
            insertion.executeUpdate();
        });

        // In processing items without time constraints main difference is in not caring about year/century and as key only name of the city is used
        JavaPairRDD<String, Tuple2<Double, Integer>> cAllRecords = cityD.mapToPair(cityRecord ->
                new Tuple2<>(cityRecord.getCity(), new Tuple2<>(cityRecord.getAvgTemp(), 1)));

        JavaPairRDD<String, Tuple2<Double, Integer>> notNullAllCrecs =
                cAllRecords.filter((Function<Tuple2<String, Tuple2<Double, Integer>>, Boolean>) obj
                        -> obj._2._1 != null);

        JavaPairRDD<String, Tuple2<Double, Integer>> cReducedAllRecs = notNullAllCrecs.reduceByKey((tuple1,tuple2) ->
                new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        JavaPairRDD<String, Double> cAverageAmongAll = cReducedAllRecs.mapToPair(getAverageByKey);
        cAverageAmongAll.foreach((item) -> {
            String city = item._1;
            Double avgTemp = item._2;
            String request = "INSERT INTO cities_whole_period (city, avgtemp) VALUES (?, ?);";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setString(1, city);
            insertion.setDouble(2, avgTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<String, Tuple2<Double, Integer>> cMaxAmongAll = cAllRecords.reduceByKey(Main::calcAbsMax);
        cMaxAmongAll.foreach((item) -> {
            String city = item._1;
            Double maxTemp = item._2._1;
            String request = "INSERT INTO cities_whole_period (maxtemp) VALUES (?) WHERE city='"+city+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, maxTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<String, Tuple2<Double, Integer>> cMinAmongAll = cAllRecords.reduceByKey(Main::calcAbsMin);
        cMinAmongAll.foreach((item) -> {
            String city = item._1;
            Double minTemp = item._2._1;
            String request = "INSERT INTO cities_whole_period (mintemp) VALUES (?) WHERE city='"+city+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, minTemp);
            insertion.executeUpdate();
        });

        // Average, max and min temperatures for each country per year, century and for the whole period.

        // Processing is performed in the same way as it was done previously for cities
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> countryRecordsByYear = countryD.mapToPair(countryRecord ->
                new Tuple2<>(new Tuple2<>(countryRecord.getCountry(), year(countryRecord.getDt())), new Tuple2<> (countryRecord.getAvgTemp(), 1)));

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> notNullCountryrecsByYear =
                countryRecordsByYear.filter((Function<Tuple2<Tuple2<String, Integer>, Tuple2<Double, Integer>>, Boolean>) obj
                        -> obj._2._1 != null);

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> countryRecsReducedByYear = notNullCountryrecsByYear.reduceByKey((tuple1,tuple2) ->
                new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> countryAverageByYear = countryRecsReducedByYear.mapToPair(getAverageByPeriod);
        countryAverageByYear.foreach((item) -> {
            String country = item._1._1;
            Integer year = item._1._2;
            Double avgTemp = item._2._1;
            String request = "INSERT INTO countries_year (country, year, avgtemp) VALUES (?, ?, ?);";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setString(1, country);
            insertion.setInt(2, year);
            insertion.setDouble(3, avgTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> countryMaxaggByYear = notNullCountryrecsByYear.reduceByKey(Main::calcMax);
        countryMaxaggByYear.foreach((item) -> {
            String city = item._1._1;
            Integer year = item._1._2;
            Double maxTemp = item._2._1;
            String request = "INSERT INTO countries_year (maxTemp) VALUES (?) WHERE country='"+city+"' AND year='"+year+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, maxTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> countryMinaggByYear = notNullCountryrecsByYear.reduceByKey(Main::calcMin);
        countryMinaggByYear.foreach((item) -> {
            String city = item._1._1;
            Integer year = item._1._2;
            Double minTemp = item._2._1;
            String request = "INSERT INTO countries_year (minTemp) VALUES (?) WHERE country='"+city+"' AND year='"+year+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, minTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> countryRecordsByCentury = countryD.mapToPair(countryRecord ->
                new Tuple2<>(new Tuple2<>(countryRecord.getCountry(), century(countryRecord.getDt())), new Tuple2<> (countryRecord.getAvgTemp(), 1)));

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> notNullCountryRecsByCentury =
                cRecordsByCentury.filter((Function<Tuple2<Tuple2<String, Integer>, Tuple2<Double, Integer>>, Boolean>) obj
                        -> obj._2._1 != null);

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> countryRecsReducedByCentury = notNullCountryRecsByCentury.reduceByKey((tuple1,tuple2) ->
                new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> countryAverageByCentury = countryRecsReducedByCentury.mapToPair(getAverageByPeriod);
        countryAverageByCentury.foreach((item) -> {
            String country = item._1._1;
            Integer century = item._1._2;
            Double avgTemp = item._2._1;
            String request = "INSERT INTO countries_century (country, century, avgtemp) VALUES (?, ?, ?);";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setString(1, country);
            insertion.setInt(2, century);
            insertion.setDouble(3, avgTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> countryMaxaggByCentury = notNullCountryRecsByCentury.reduceByKey(Main::calcMax);
        countryMaxaggByCentury.foreach((item) -> {
            String country = item._1._1;
            Integer century = item._1._2;
            Double maxTemp = item._2._1;
            String request = "INSERT INTO countries_century (maxTemp) VALUES (?) WHERE country='"+country+"' AND century='"+century+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, maxTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> countryMinaggByCentury = notNullCountryRecsByCentury.reduceByKey(Main::calcMin);
        countryMinaggByCentury.foreach((item) -> {
            String country = item._1._1;
            Integer century = item._1._2;
            Double minTemp = item._2._1;
            String request = "INSERT INTO countries_century (minTemp) VALUES (?) WHERE country='"+country+"' AND century='"+century+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, minTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<String, Tuple2<Double, Integer>> countryAllRecords = countryD.mapToPair(countryRecord ->
                new Tuple2<>(countryRecord.getCountry(), new Tuple2<>(countryRecord.getAvgTemp(), 1)));

        JavaPairRDD<String, Tuple2<Double, Integer>> notNullAllCountryRecs =
                countryAllRecords.filter((Function<Tuple2<String, Tuple2<Double, Integer>>, Boolean>) obj
                        -> obj._2._1 != null);

        JavaPairRDD<String, Tuple2<Double, Integer>> cReducedAllCountryRecs = notNullAllCountryRecs.reduceByKey((tuple1,tuple2) ->
                new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        JavaPairRDD<String, Double> countryAverageAmongAll = cReducedAllCountryRecs.mapToPair(getAverageByKey);
        countryAverageAmongAll.foreach((item) -> {
            String country = item._1;
            Double avgTemp = item._2;
            String request = "INSERT INTO countries_whole_period (country, avgtemp) VALUES (?, ?);";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setString(1, country);
            insertion.setDouble(2, avgTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<String, Tuple2<Double, Integer>> countryMaxAmongAll = notNullAllCountryRecs.reduceByKey(Main::calcAbsMax);
        countryMaxAmongAll.foreach((item) -> {
            String country = item._1;
            Double maxTemp = item._2._1;
            String request = "INSERT INTO countries_whole_period (maxtemp) VALUES (?) WHERE country='"+country+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, maxTemp);
            insertion.executeUpdate();
        });

        JavaPairRDD<String, Tuple2<Double, Integer>> countryMinAmongAll = notNullAllCountryRecs.reduceByKey(Main::calcAbsMin);
        countryMinAmongAll.foreach((item) -> {
            String country = item._1;
            Double minTemp = item._2._1;
            String request = "INSERT INTO countries_whole_period (mintemp) VALUES (?) WHERE country='"+country+"'";
            PreparedStatement insertion = connection.prepareStatement(request);
            insertion.setDouble(1, minTemp);
            insertion.executeUpdate();
        });
    }

    // Reducer for searching min temperature among all items without time windows
    private static Tuple2<Double, Integer> calcAbsMin(Tuple2<Double, Integer> val1, Tuple2<Double, Integer> val2) {
        if (val1._1 < val2._1)
            return new Tuple2(val1._1, 1);
        else
            return new Tuple2(val2._1, 1);
    }

    // Reducer for searching max temperature among all items without time windows
    private static Tuple2<Double, Integer> calcAbsMax(Tuple2<Double, Integer> val1, Tuple2<Double, Integer> val2) {
        if (val1._1 > val2._1)
            return new Tuple2(val1._1, 1);
        else
            return new Tuple2(val2._1, 1);
    }

    // Reducer for calculating average temperature among items with time windows storing counter and total sum of temperature per city/country
    private static PairFunction<Tuple2<Tuple2<String, Integer>, Tuple2<Double, Integer>>,
            Tuple2<String, Integer>, Tuple2<Double, Integer>> getAverageByPeriod = (tuple) -> {
        Tuple2<Double, Integer> val = tuple._2;
        Double total = val._1;
        Integer count = val._2;
        return new Tuple2<>(tuple._1, new Tuple2<>((total / count), count));
    };

    // Reducer for calculating average temperature among items without time windows
    private static PairFunction<Tuple2<String, Tuple2<Double, Integer>>,String,Double> getAverageByKey = (tuple) -> {
        Tuple2<Double, Integer> val = tuple._2;
        Double total = val._1;
        Integer count = val._2;
        return new Tuple2<>(tuple._1, total / count);
    };

    // Reducer for searching min temperature among items with time windows
    private static Tuple2<Double, Integer> calcMin(Tuple2<Double, Integer> value1, Tuple2<Double, Integer> value2) {
        if (value1._1 == null)
            return value2;
        else if (value2._1 == null)
            return value1;
        else if (value1._1 < value2._1)
            return value1;
        else
            return value2;
    }

    // Reducer for searching max temperature among items with time windows
    private static Tuple2<Double, Integer> calcMax(Tuple2<Double, Integer> value1, Tuple2<Double, Integer> value2) {
        if (value1._1 == null)
            return value2;
        else if (value2._1 == null)
            return value1;
        else if (value1._1 > value2._1)
            return value1;
        else
            return value2;
    }

    // Extracting year from timestamp
    private static int year(Timestamp dt) {
        long timestamp = dt.getTime();
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestamp);
        return cal.get(Calendar.YEAR);
    }

    // Extracting century from timestamp
    private static int century(Timestamp dt) {
        long timestamp = dt.getTime();
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestamp);
        int year = cal.get(Calendar.YEAR);
        return year / 100 + 1;
    }

    // Excluding header from rows before processing
    private static JavaRDD<String> prepareData(JavaRDD<String> data) {
        final String header = data.first();
        JavaRDD<String> d = data.filter((Function<String, Boolean>) s -> !s.equalsIgnoreCase(header));
        return d;
    }

    // JDBC driver initialization
    public static Connection initConection(String url, String name, String password)
            throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection(url, name, password);
    }
}
