/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cms.reef.data.loading;

import com.microsoft.reef.annotations.audience.TaskSide;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.util.Pair;
import com.microsoft.reef.task.Task;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The task that iterates over the data set to count the number of records.
 * Assumes TextInputFormat and that records represent lines.
 */
@TaskSide
public class LineCountingTask implements Task {

  //TODO convert data structure to proper one (for distributed system)
	
	
  // hypothesis function
  public double[] hFunc(double[][] Data, double[] param){
	  int card = Data.length;
	  
	  double[] res = new double[card];
	  
	  for (int i=0; i<card;i++){
		  for (int j=0; j<param.length-1;j++){
			  res[i] += param[j]*(Data[i][j]);
		  }  
	  }
	  
	  return res;
  }
  
  // cost Function (1/cardinality) * sigma {(h(x_i) - y_i )*x_j}
  public double[] jFunc(double[][] Data, double[] param){
	  int dim = param.length;
	  int card = Data.length;
	  double[] res = new double[dim]; 
	  double[] tempR = hFunc(Data, param);	// calc hypothesis function at this moment  
	  
	  for (int j=0; j<dim; j++){		  
		  double tempRes = 0;
		  for (int i=0; i<card;i++){
			  tempRes += (double) (1/card) * (tempR[i] - (Data[i][dim-1]) ) * (Data[i][j]);
		  }
		  res[j] = tempRes;
	  }
	  
	  /* 
	  for (int i = 0; i<dim; i++){
		  System.out.print(res[i] + " ");
	  }
	  */
	  return res;
  }
  
  // update theta function
  
  public double[] uFunc(double[] bParam, double[] costRes, double alpha  ){
	  int dim = bParam.length;
	  double[] newParam = new double[dim];
	  
	  for (int i =0; i<dim; i++){
		  newParam[i] = bParam[i] - alpha*costRes[i];
	  }
	  
	  return newParam;
	  
  }
    
  private static final Logger LOG = Logger.getLogger(LineCountingTask.class.getName());

  private final DataSet<LongWritable, Text> dataSet;

  @Inject
  public LineCountingTask(final DataSet<LongWritable, Text> dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.FINER, "LineCounting task started");
    
    //initialize variables
    int numEx = 0;
    String totalData[][];
    int dim = 0;
    double[] params;
    
    // min, max for normalizing data set
    
    
 
    // first scan for initialize
    for (final Pair<LongWritable, Text> keyValue : dataSet) {
        // LOG.log(Level.FINEST, "Read line: {0}", keyValue);
    	if (numEx == 1){
    		dim = keyValue.second.toString().split("\\s+").length;
    	}
        ++numEx;
      }
    
    float [] min = {1000,1000,1000,1000,1000};
    float [] max = new float[dim-1];
    params = new double[dim-1];
    params[0] = 0.5; params[1] = 1; params[2] = 1; params[3] = 0.5; params[4] = 1;
    
    /*
    for(int j = 0; j<params.length;j++){
    	//params[j] = (float) Math.random();
    	params[j] = 1;
    }
      
    */
    totalData = new String[numEx][dim];
    int idx = 0;
    for (final Pair<LongWritable, Text> keyValue : dataSet) {
    	totalData[idx] = keyValue.second.toString().split("\\s");
    	
    	for (int j=0;j<5;j++){
    		if( Float.parseFloat(totalData[idx][j]) < min[j]){
       			min[j] = Float.parseFloat(totalData[idx][j]); 
       		}
    		if( Float.parseFloat(totalData[idx][j]) > max[j]){
    			max[j] = Float.parseFloat(totalData[idx][j]); 
    		}
    	}	
    	idx++;
    }
    // in case of data normalization 0~1
    idx = 0;
    double[][] normData;
    normData = new double[numEx][dim];
    for (;idx<numEx;idx++) {
    	
    	for (int j=0; j<5; j++){
    		normData[idx][j] = (Float.parseFloat(totalData[idx][j]) -min[j])/(max[j]-min[j]) ;
    	}
    }
    
    
    /*
    idx = 0;
    double[][] normData;
    normData = new double[numEx][dim];
    for (;idx<numEx;idx++) {
    	for (int j=0; j<5; j++){
    		normData[idx][j] = Float.parseFloat(totalData[idx][j]);
    	}
    }
    */
    
    for (int j=0; j<5;j++){
    	System.out.print("max: " + max[j]);
    	System.out.println(" min: "+ min[j]);
    }
    // float[] tempRes = hFunc(totalData, params);

       
    double[] tempR = new double[dim-1];
    double[] newParam = new double[dim-1];
    //double[] tempParams = new double[dim-1];
    
    for (int i =0; i < dim-1; i++){
    	System.out.print(params[i] + "  ");
    }
    
    for (int idx2= 0; idx2<300; idx2++){
    	tempR = jFunc(normData, params);
    	newParam = uFunc(params, tempR, 0.01);
        for (int i =0; i < dim-1; i++){
        	System.out.print(newParam[i] + "  ");
        	
        }
        System.out.println();

    	params = newParam;
    }
    
    /*
    tempR = jFunc(totalData, params);
    newParam = uFunc(params, tempR, (double) 0.1);
    */
    
    for (int i =0; i < dim-1; i++){
    	System.out.print(newParam[i] + "  ");
    }
    System.out.println();

    
    //    tempParams = uFunc(params, jFunc(totalData, params), 0.1);
    
    /*
    for (int idx2=0; idx2<1000; idx2++){
    	// calc based on hypothesis
    	
    	
    	tempParams = uFunc(params, jFunc(totalData, params), 0.1);
    	params = tempParams;
        	
    }
    */
    /*
    for (int i =0; i < dim; i++){
    	System.out.print(tempParams[i] + " ");
    }
    */
    	
    LOG.log(Level.FINER, "LineCounting task finished: read {0} lines", numEx);
    System.out.println("Counted line num is: " + numEx);
    
    return Integer.toString(numEx).getBytes();
  }

}