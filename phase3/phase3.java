import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkConf

public class Phase3 implements Serializable{


    public static float standardDev(float mean , int total_cells , float sum_x_squared) 
    {
    	float standDev = Math.sqrt((sum_x_squared / total_cells ) - Math.pow(mean,2));
    	return standDev;
    }

    public static float getis_OrdValue()
    {

    }


    //this function will be used on one day of data - need to add the data to this function delceration 
    public static void groupCellData(PointRDD objectRDD)
    {
    	//constants for cell boundires 
    	float latitude_min = 40.5;
    	float latitude_max = 40.9;
    	float longitude_min = 73.7;
    	float longitude_max = 74.25;
    	float cell_length = .01;

    	float lat_boundry = 0.0;
    	float long_boundry = 0.0;

    	int total_cells = 0; 

    	//mean Value 
    	float mean_Value = 0.0;

    	//standard dev values 
    	float x_squared_sum = 0.0;
    	float standard_Dev_value =0.0;

    	//for each latitude itterate over all cells
    	for(int i=latitude_min; i <= (latitude_max - cell_length); i=i+cell_length)
    	{
    		//itterate all longitude blocks 
    		for(int j=longitude_min; j <= (longitude_max - cell_length); j=j+cell_length)
    		{
    			lat_boundry = i + cell_length;
    			long_boundry = j + cell_length;
    			//collect all values within the cell blk
    			//set boundry envelope 
    			val queryEnvelope=new Envelope (long_boundry,j,i,lat_boundry);
    			//query for the values within the envelope 
    			val points_in_cell = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, 0).getRawPointRDD().count();
    			mean_Value += points_in_cell;
    			//square and sum the value for this cell - used for standard dev 
    			x_squared_sum += Math.pow(points_in_cell,2);

    			total_cells++;

    		} // end of inner for loop

    		total_cells++;
    	}//end of for loop

    	//calcualte the mean value for all the cells 
    	mean_Value = mean_Value / total_cells;

    	//calc standard dev 
    	standard_Dev_value = standardDev(mean_Value,total_cells,x_squared_sum);

    }

}
