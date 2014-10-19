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

  //TODO convert data structure to proper one (for distributed system, e.g. array to DenseVector in shimoga )
	
	
  // hypothesis function
  public double[] hFunc(double[][] Data, double[] param){
	  int card = Data.length;
	  
	  double[] res = new double[card];
	  
	  for (int i=0; i<card;i++){
		  for (int j=0; j<param.length;j++){
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
			  tempRes += (tempR[i] - (Data[i][dim]) ) * (Data[i][j]);
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
  
  public int converge(double[] aparam, double[] bparam){
	  //TODO: add assert statement // modify to boolean
	  double dev =0;
	  if(aparam.length != bparam.length){
		  System.out.println("Assert!");
	  }
	  else{
		  int dim = aparam.length;

		  for (int i=0; i<dim;i++){
			  dev += Math.pow(aparam[i]-bparam[i],2);
		  }
	  }
	  if (dev < 0.00001){
		  return 1;
	  }
	  else{
		  return 0;
	  }
	  
  }
  
  // update theta function
  
  public double[] uFunc(double[] bParam, double[] costRes, double alpha  ){
	  int dim = bParam.length;
	  double[] newParam = new double[dim];
	  double m = costRes.length;
	  for (int i =0; i<dim; i++){
		  newParam[i] = bParam[i] - alpha*costRes[i]/m;
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
    LOG.log(Level.FINER, "DataLoading task started");
    
    //initialize variables
    int numEx = 0;
    String totalData[][];
    int dim = 0;
    double[] params;
    
    
    // first scan for initialize
    for (final Pair<LongWritable, Text> keyValue : dataSet) {
        // LOG.log(Level.FINEST, "Read line: {0}", keyValue);
    	if (numEx == 1){
    		dim = keyValue.second.toString().split("\\s+").length;
    	}
        ++numEx;
      }
    // min, max for normalizing data set    
    double [] min = {1000,1000,1000,1000,1000,1000};
    double [] max = new double[dim];
    double [] sum = new double[dim];
    params = new double[dim-1];
        
    for(int j = 0; j<params.length;j++){
    	params[j] = Math.random();
//    	params[j] = 1;
    }
      
    totalData = new String[numEx][dim];
    int idx = 0;
    for (final Pair<LongWritable, Text> keyValue : dataSet) {
    	totalData[idx] = keyValue.second.toString().split("\\s");
    	
    	for (int j=0;j<dim;j++){
    		double d = Double.parseDouble(totalData[idx][j]);
    		if( d < min[j]){
       			min[j] = d; 
       		}
    		if( d > max[j]){
    			max[j] = d; 
    		}
    		sum[j] += d;
    	}	
    	idx++;
    }
    // in case of data normalization 0~1
    idx = 0;
    double[][] normData;
    double [] avg = new double[dim];
    for (int i=0; i<dim; i++){
    	avg[i] = sum[i] / numEx;
    }
    normData = new double[numEx][dim];
    for (;idx<numEx;idx++) {
    	
    	for (int j=0; j<dim; j++){
    		normData[idx][j] = (Double.parseDouble(totalData[idx][j]) - avg[j])/(max[j]-min[j]) ;
    		
    	}
    }
    LOG.log(Level.FINER, "Data Loading & Normalization Done");
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
    
    for (int j=0; j<dim;j++){
    	System.out.print("max, min, avg" + max[j]+ " " + min[j] + " " + avg[j]);
    	System.out.println("");
    }
    
    // float[] tempRes = hFunc(totalData, params);
       
    double[] tempR = new double[dim-1];
    double[] newParam = new double[dim-1];
    //double[] tempParams = new double[dim-1];
    
    
    
    
    
    
    System.out.print("Initial params: ");
    for (int i =0; i < dim-1; i++){
    	System.out.print(params[i] + "  ");
    }
    System.out.println("");
    
    for (int idx2= 0; idx2<100; idx2++){
    	tempR = jFunc(normData, params);
    	newParam = uFunc(params, tempR, 1);
        for (int i =0; i < dim-1; i++){
        	System.out.print(newParam[i] + "  ");
        	
        }
        System.out.println();
        if (converge(params,newParam) == 1){
        	System.out.println("#Iterate: " + (idx2+1));
        	break;        	
        }
        else{
        	params = newParam;
        }
    }
    
    /*
    tempR = jFunc(totalData, params);
    newParam = uFunc(params, tempR, (double) 0.1);
    */
 // for check
    double[] re = new double[numEx];
    re = hFunc(normData, newParam);
    
    
    for (int i =0; i < dim-1; i++){
    	System.out.print(newParam[i] + "  " + re[i]);
    }
    System.out.println();

        	
    LOG.log(Level.FINER, "Linear Regression task finished: read {0} lines", numEx);

    return Integer.toString(numEx).getBytes();
  }

}