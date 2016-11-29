def gVal( cellX:Int, cellY:Int, day:Int, mean:Double, stdDev:Double, totalCells :Int, subCellVal:Int ) : Double = {
      var g_val:Int = 0
      var wVal:Int = 0 

      wVal = weightNeighbor(cellX,cellY,day)

      g_val = (wVal * subCellVal - mean * wVal) / (stdDev * math.sqrt((totalCells * wVal -  math.pow(wVal, 2)) / (totalCells - 1)))

      return g_val
   }




def weightNeighbor( x:Int, y:Int, day:Int) : Int = {
      var neighbor_val:Int = 0
    if(day == 1 || day == 31)
    {
		//check if it is a corner cube
		if( (x == 0 && y == 0) || (x ==39 && y == 0) || (x == 0 && y == 54) || (x == 39 && y == 54 )){
			neighbor_val = 7;
		}
		else if( (x == 0 && y != 0) || (x ==39 && y != 0) || (x == 0 && y != 54) || (x == 39 && y =! 54 ) ){
			neighbor_val =11;
		}
		else{
			neighbor_val = 17; 
		}
    }
    else
    {
		//check if it is a corner cube
		if( (x == 0 && y == 0) || (x ==39 && y == 0) || (x == 0 && y == 54) || (x == 39 && y == 54 )){
			neighbor_val = 11;
		}
		else if( (x == 0 && y != 0) || (x ==39 && y != 0) || (x == 0 && y != 54) || (x == 39 && y =! 54 ) ){
			neighbor_val =17;
		}
		else{
			neighbor_val = 26;  
		}
    }

      return neighbor_val
   }
