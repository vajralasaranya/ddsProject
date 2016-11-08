import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkConf

public class Phase2 implements Serializable{

		
    public static JavaPairRDD<Envelope, HashSet<Point>> SpatialJoinQueryUsingCartesianProduct(PointRDD objectRDD, RectangleRDD rectangleRDD) {

        ArrayList<Envelope> queryEnvelope = queryWindowRDD.getRawRectangleRDD().takesample();

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        int loopRectTimes = queryWindowRDD.getRawRectangleRDD().count();

        List<RectangleRDD> rectangleList = new ArrayList<RectangleRDD>();
        
        for(int i =0; i < loopRectTimes; i++)
        {
            RectangleRDD resultOne = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope[i], 0);
            rectangleList.add(resultOne);
        }
    		
    	//Collect all results;

        JavaPairRDD<Envelope, HashSet<Point>> restuls = sc.parallelizePairs(rectangleList);    

    	//Return the result RDD;
    	return results; 

    }

}
