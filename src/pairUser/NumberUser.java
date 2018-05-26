package pairUser;

import org.apache.hadoop.hive.ql.exec.UDF;

public class NumberUser extends UDF{

	private int  counter = 0;
    private String last_key = "";
    
    public int evaluate(final String key){
    	
      if ( !key.equalsIgnoreCase(this.last_key) ) {
         this.counter++;
         this.last_key = key;
      }
      return this.counter;
      
    }
	
}
