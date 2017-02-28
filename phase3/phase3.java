/*
Arizona State University
DDS Project
This code is for a class project. 
Copying this code would lead to plaigarism and any code copied from here should be cited with authors names.

Code Authors:
1) Rajesh
2) Saranya
3) Stephannie
4) Jayanth
5) Sandeep
*/

import java.util.*;
import java.util.regex.Pattern;
 
import org.antlr.runtime.IntStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.util.StatCounter;
import scala.Serializable;
import scala.Tuple2;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
 
 
class TupleMapLongComparator implements Comparator<String>, Serializable {
    @Override
    public int compare(String tuple1, String tuple2) {
        Double g1 = Double.valueOf(tuple1.split(",")[0]);
        Double g2 = Double.valueOf(tuple2.split(",")[0]);
        return -g1.compareTo(g2);
    }
}
 
public final class phase3 {
    /*
    final class String {
        Integer neibhoursCount;
        Integer numNeibhours;
        String exists;
        String(Integer a, Integer b,String c) {
            this.neibhoursCount=a;
            this.numNeibhours=b;
            this.exists=c;
        }
    }
    */
    private JavaSparkContext sc;
    private static Double step = 0.01;
    private static Double xstart = 40.5;
    private static Double xend = 40.9;
    private static Double ystart = -74.25;
    private static Double yend = -73.7;
 
    private static int xstart1 = 4050;
    private static int xend1 = 4090;
    private static int ystart1 = -7425;
    private static int yend1= -7370;
 
    private static int zstart = 1;
    private static int zend = 31;
 
    public static double getis_OrdValue(String s,Double total_cells,Double Strdev,Double mean)
    {
        // x,y,z,neihboursCount,numNeighbours
        //System.out.println("gscore : "+s);
        String[] a = s.split(",");
        Integer xcell = Double.valueOf(a[0]).intValue();
        Integer ycell = Double.valueOf(a[1]).intValue();
        Integer zcell = Double.valueOf(a[2]).intValue();
        double neighboursCount = Double.valueOf(a[3]);
        double numNeighbours = Double.valueOf(a[4]);
        double g_val = (neighboursCount  - mean * numNeighbours);
        //System.out.println(g_val);
        //bottom half eq calc
        double under_sqrt_eq = ((total_cells *numNeighbours) - Math.pow(numNeighbours,2) )/(total_cells - 1);
        double tmp_bottom_eq = Strdev * Math.sqrt(under_sqrt_eq);
        //System.out.println(tmp_bottom_eq);
        //now combine top and bottom to get g value
        g_val = g_val / tmp_bottom_eq;
        //System.out.println("Gscore:"+xcell+","+ycell+","+zcell+","+neighboursCount+","+numNeighbours+","+g_val);
        //System.out.println(s+"-------->>>>gscore : "+g_val);
        return g_val;
    }
 
    public static String getCellx(String lat) {
        Double diff = xstart-xend;
        Double val = Double.parseDouble(lat);
        if (val < xstart || val > xend) {
            return "";
        }
        Double d = val-xstart;
        d=val*100;
        Integer cellXval = (int) Math.floor(d);
        return Integer.toString(cellXval);
    }
 
    public static String getCelly(String lon) {
        Double val = Double.parseDouble(lon);
        if (val < ystart || val > yend) {
            return "";
        }
        Double d = val-ystart;
        d=(val)*100;
        Integer cellYval = (int) -Math.floor(d);
        return Integer.toString(cellYval);
    }
 
 
    public static List<String> myRun(JavaSparkContext sc,String inputLocation,String outputLocation) {
 
        JavaRDD<String> lines = sc.textFile(inputLocation);
        JavaRDD<String> wordsList = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String s) {
                        String[] a=s.split(",");
                        String pickupLat = getCellx(a[6]);
                        String pickupLon = getCelly(a[5]);
                        if (pickupLat.equals("") || pickupLon.equals("")) {
                            List<String> resKey1 =  Arrays.asList();
                            return resKey1;
                        }
                        String dateTimeString = a[1];
                        // 4075,7399
 
                        String dayString=dateTimeString.split(" ")[0].split("-|/")[2];
                        int dayVal = Integer.parseInt(dayString)-1;
                        String key = pickupLat+","+pickupLon+","+dayVal;
                        List<String> resKey = Arrays.asList(key);
                        //System.out.println(s+"----"+pickupLat+","+pickupLon+","+dayString);
                        if (pickupLat.equals("4075") && pickupLon.equals("7399")) {
                            //System.out.println(s+"--------------"+key);
                        }
                        //System.out.print(a[6]+":"+a[5]+":"+dayString+"-------------");
                        //System.out.println(resKey);
                        return resKey;
                    }
                }
        );
        JavaPairRDD<String, Integer> wordPairs = wordsList.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> wordCounts = wordPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });
         
        //wordCounts.saveAsTextFile("/tmp/sampleOutput.txt");
        //System.out.println("-------------->1");
 
        JavaRDD<String> countSquaredSum = wordCounts.flatMap(
                new FlatMapFunction<Tuple2<String,Integer>, String>() {
                    public Iterable<String> call(Tuple2<String,Integer> s) {
                        String key = s._1+","+s._2.toString();
                        List<String> l=Arrays.asList(key);
                        return l;
                    }
                }
        );
        JavaPairRDD<String, Double> SquaredPairs = countSquaredSum.mapToPair(new PairFunction<String, String, Double>() {
            public Tuple2<String, Double> call(String s) {
                Double val = Double.parseDouble(s.split(",")[3]);
                return new Tuple2<String, Double>("SquaredSum", val*val);
            }
        });
 
        JavaPairRDD<String, Double> sqredSumCount = SquaredPairs.reduceByKey(new Function2<Double, Double, Double>() {
            public Double call(Double a, Double b) { return a + b; }
        });
 
        List<Double> spairs = sqredSumCount.values().toArray();
        double ssum = (double) spairs.get(0);
 
        //System.out.println("squaredSum :"+ssum);
 
        final int total = 31*(int) Math.round((xend-xstart)/step)*(int) Math.round(((yend-ystart)/step));
        //System.out.println("Total :"+total);
        int sum = (int)wordPairs.count();
        //System.out.println("sum : "+sum);
        final double mean = (double)(sum*1.0)/(total*1.0);
        //System.out.println("mean : "+mean);
        Double varOrig = (ssum/total)-(mean*mean);
        //System.out.println("varOrig : "+varOrig);
        final Double sdeviance = Math.sqrt(varOrig);
        //System.out.println("sdeviance : "+sdeviance);
        // we get mean and S here;
        //System.out.println("-------------->>>>"+wordCounts.count());
        JavaRDD<String> gscoreMapList = wordCounts.flatMap(
                new FlatMapFunction<Tuple2<String,Integer>, String>() {
                    public Iterable<String> call(Tuple2<String,Integer> s) {
                        String[] slist = s._1.split(",");
                        int x = Integer.parseInt(slist[0]);
                        int y = Integer.parseInt(slist[1]);
                        Integer z = Integer.parseInt(slist[2]);
                        List<String> l=new LinkedList<String>();
                        //System.out.println(s._1+","+s._2);
                        for(int i =-1; i<2;i++) {
                            for(int j=-1; j<2;j++) {
                                for(int k=-1; k<2;k++) {
                                    int x1 = x+ i;
                                    int y1 = y + j;
                                    int z1 = z + k;
                                    //System.out.println(s+"@@@@@@@@@@@@@"+x1+","+y1+","+z1+","+isValid(x1,-y1,z1)+","+i+","+j+","+k);
                                    if(isValid(x1,-y1,z1)){
                                        String exists = "U";
                                        if(i == 0 && j ==0 && k ==0)
                                            exists = "E";
                                        Integer xv = x1;
                                        Integer yv = y1;
                                        Integer zv = z1;
                                        String t = xv+","+yv+","+zv+","+s._2+","+exists;
                                        l.add(t);
                                    }
                                }
                            }
                        }
                        return l;
                    }
                }
        );
 
        JavaPairRDD<String, String> gscoreMapListReduce = gscoreMapList.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String a[] = s.split(",");
                Integer val = Integer.parseInt(a[3]);
                String exists = a[4];
                String t = a[0]+","+a[1]+","+a[2];
                String m = val+","+1+","+exists;
                //Tuple2<Integer,Integer> tval= new Tuple2<Integer,Integer>(val,1,exists);
                //System.out.println(s+"------->>>"+t+":tval:"+val);
                return new Tuple2<String, String>(t, m);
            }
        });
 
        JavaPairRDD<String, String> gscoreMapListValues = gscoreMapListReduce.reduceByKey(new Function2<String, String, String>() {
            public String call(String a, String b) {
                String exists="U";
                String[] aData = a.split(",");
                String[] bData = b.split(",");
                if (aData[2].equals("E") || bData[2].equals("E")) {
                    exists = "E";
                }
                Integer neibhoursCount = Integer.parseInt(aData[0])+Integer.parseInt(bData[0]);
                Integer numNeibhours = Integer.parseInt(aData[1])+Integer.parseInt(bData[1]);
                String m = neibhoursCount+","+numNeibhours+","+exists;
                //Tuple2<Integer,Integer> ans= new Tuple2<Integer,Integer>(a._1+b._1,a._2+b._2);
                return m;
            }
        });
        JavaPairRDD<String, String> gscoreMapListValuesFilter = gscoreMapListValues.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String[] aData = stringStringTuple2._2.split(",");
                return aData[2].equals("E");
            }
        });
        //gscoreMapListValuesFilter.coalesce(1).saveAsTextFile("/tmp/1.txt");
        //wordCounts.coalesce(1).saveAsTextFile("/tmp/2.txt");
        //System.out.println("-------------->>>>"+gscoreMapListValues.count());
        //System.out.println("-------------->>>>"+gscoreMapListValuesFilter.count());
        JavaRDD<String> countsG = gscoreMapListValuesFilter.flatMap(
                new FlatMapFunction<Tuple2<String,String>, String>() {
                    public Iterable<String> call(Tuple2<String,String> s) {
                        // x,y,z,neihboursCount,numNeighbours
                        //spatialWeight()
                        String a[] = s._1.split(",");
                        //Integer x = (int) Math.floor((Double.parseDouble(a[0])*step-xstart)/step);
                        //Integer y = (int) Math.floor((-(Double.parseDouble(a[1])*step)-ystart)/step);
                        //Integer z = Integer.parseInt(a[2]);
                        //int neighbours = spatialWeight(x,y,z);
                        String[] aData = s._2.split(",");
                        int neighbours = Integer.parseInt(aData[1]);
                        int neibhoursCount=Integer.parseInt(aData[0]);
                        //System.out.println(s._1+"---------------------@@@@@@@"+x+","+y+","+z+","+neighbours+","+neibhoursCount);
                        String key = s._1+","+neibhoursCount+","+neighbours;
                        //System.out.println("<<<<<<<<-------"+key);
                        List<String> l=Arrays.asList(key);
                        return l;
                    }
                }
        );
        JavaPairRDD<String, Double> pairs2 = countsG.mapToPair(new PairFunction<String, String, Double>() {
            public Tuple2<String, Double> call(String s) {
                Double gscore = getis_OrdValue(s,total*1.0,sdeviance,mean);
                String key = gscore.toString()+","+s;
                //System.out.println("<<<<--->>>"+key);
                // score,x,y,z,neihboursCount,numNeighbours
                //System.out.println("----------->>>>>>>"+s+","+gscore);
                return new Tuple2<String, Double>(key, gscore);
            }
        });
        JavaPairRDD<String, Double> sorted = pairs2.sortByKey(new TupleMapLongComparator());
        int limit = 50;
        List<Tuple2<String,Double>> ans=sorted.take(limit);
        String ansString ="";
        List<String> ans1=new ArrayList<String>();
        for (Tuple2<String,Double> i : ans) {
            //System.out.println("Val:"+i.toString());
            // score,x,y,z,neihboursCount,numNeighbours
            String[] k = i._1.split(",");
            Double xVal = Double.parseDouble(k[1])*step;
            String x = xVal.toString();
            x=k[1];
            Double yVal = -(Double.parseDouble(k[2])*step);
            String y = yVal.toString();
            y="-"+k[2];
            String z = k[3];
            String kans = x+","+y+","+z+","+i._2.toString();
            ansString+=kans+"\n";
            ans1.add(kans);
        }
        JavaRDD values1 = sc.parallelize(ans1);
        values1.coalesce(1).saveAsTextFile(outputLocation);
        for(String ans2 : ans1) {
            System.out.println(ans2);
        }
        return ans1;
        //JavaPairRDD<String, Integer> counts1 = pairs1.reduceByKey(new Function2<Integer, Integer, Integer>() {
        //    public Integer call(Integer a, Integer b) { return a + b; }
        //});
        //List<Tuple2<String,Integer>> e = counts1.collect();
        //return mean
        //return e.get(0)._2;
    }
 
    private static boolean isValid(int x, int y, int z) {
        if (x< xstart1 || x> xend1) {
            return false;
        }
        if (y< ystart1 || y> yend1) {
            return false;
        }
        if (z< zstart || z> zend) {
            return false;
        }
        return true;
    }
 
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Phase3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputLocation="/home/rajesh/ddsData/yellow_tripdata_2015-01.csv";
        //inputLocation = args[0];
        //System.out.println(args[0]+","+args[1]);
        inputLocation="/home/rajesh/Desktop/small.csv";
        inputLocation=args[0];
        String outputLocation="/tmp/output1.txt";
        outputLocation=args[1];
        List<String> ans=myRun(sc,inputLocation,outputLocation);
    }
}
