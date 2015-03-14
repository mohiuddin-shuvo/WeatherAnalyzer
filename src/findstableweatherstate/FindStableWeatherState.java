/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package findstableweatherstate;

import mapred.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

/**
 * 
 * @author shuvo
 */
public class FindStableWeatherState {

	private String inputPathStation;
	private String inputPathReadings;

	public void setInputPathStation(String inputPaths) {
		this.inputPathStation = inputPaths;
	}

	public void setInputPathReadings(String inputPaths) {
		this.inputPathReadings = inputPaths;
	}

	public String getInputPathStation() {
		return inputPathStation;
	}

	public String getInputPathReadings() {
		return inputPathReadings;
	}

	private String outputPath;

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	private RunningJob runningJob;

	public RunningJob getRunningJob() {
		return runningJob;
	}

	public String call() throws Exception {
		
		Path firstOutputPath = new Path("input/firstOutput");
        Path secondOutputPath = new Path("input/secondOutput");
        
		JobConf job = new JobConf();
		job.setJarByClass(getClass());
		job.setJobName("invertedindex");

		 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

         
		job.setReducerClass(JoinReducer.class);
		
	     MultipleInputs.addInputPath(job, new Path(getInputPathStation()), TextInputFormat.class, StationMapper.class);
         MultipleInputs.addInputPath(job, new Path(getInputPathReadings()), TextInputFormat.class, ReadingsMapper.class);
		
		 
         
		 		
		 FileOutputFormat.setOutputPath(job, firstOutputPath);
		 

		 
		JobConf job2 = new JobConf();
		job2.setJarByClass(getClass());
		job2.setJobName("secondJob");
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		//job2.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
		FileInputFormat.setInputPaths(job2,  firstOutputPath);

		job2.setMapperClass(CalculateMinMaxTemperatureMapper.class);
		
		job2.setReducerClass(CalculateMaxMinTemperatureReducer.class);
		if (getOutputPath() != null) {			
			FileOutputFormat.setOutputPath(job2, secondOutputPath);
		}
		 
		
		JobConf job3 = new JobConf();
		job3.setJarByClass(getClass());
		job3.setJobName("thirdJob");
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		//job2.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
		FileInputFormat.setInputPaths(job3,secondOutputPath);

		job3.setMapperClass(SortStateMapper.class);
		
		job3.setReducerClass(SortStateReducer.class);
		if (getOutputPath() != null) {			
			FileOutputFormat.setOutputPath(job3, new Path(getOutputPath()));
		}
		 
		//JobClient.runJob(job);
		//JobClient.runJob(job2);
		JobClient.runJob(job3);
		
		return "";
	}

	/**
	 * This method is executed by the workflow
	 */
	public static void initCustom(JobConf conf) {
		// Add custom initialisation here, you may have to rebuild your project
		// before
		// changes are reflected in the workflow.
		// conf.setOutputValueClass(DocSumWritable.class);

	}

	@SuppressWarnings("unchecked")
	// <editor-fold defaultstate="collapsed"
	// desc="Generated Code">//GEN-BEGIN:initJobConf
	public static void initJobConf(JobConf conf) {
		// Generating code using Karmasphere Protocol for Hadoop 0.18
		// CG_GLOBAL

		// CG_INPUT_HIDDEN
		conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);

		// CG_MAPPER_HIDDEN
		conf.setMapperClass(StationMapper.class);

		// CG_MAPPER
		conf.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);

		//conf.setReducerClass(IndexReducer.class);

		// CG_REDUCER
		// conf.setNumReduceTasks(1);
		conf.setOutputKeyClass(Text.class);

		// CG_OUTPUT_HIDDEN
		conf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

		conf.setMapOutputValueClass(Text.class);
		conf.setOutputValueClass(Text.class);
		// CG_OUTPUT

		// Others
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception {
		// TODO code application logic here
		FindStableWeatherState job = new FindStableWeatherState();
		//job.setInputPaths(".");
		//job.setOutputPath(".");
		
		if (args.length >= 1)
		{
			job.setInputPathReadings(args[0]);
			System.out.println(args[0]);
		}
		if (args.length >=2)
		
			job.setInputPathStation(args[1]);
		if (args.length >=3) 
			job.setOutputPath(args[2]);
		job.call();
		/* Wish we didn't have to reproduce code from runJob() here. */
		// job.getRunningJob().waitForCompletion();
	}

}
