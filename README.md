# WeatherAnalyzer
For this project, we have two datasets. The first provides station information for weather stations across the world. The second provides individual recordings for the stations over a 4-year period. The goal of the project is to find out which states in the US have the most stable temperature (i.e. their hottest month and coldest month have the least difference)
Three Phase chained Map Reduce Jobs was used for this project. In the first phase it joined the two datasets, in the second phase it aggregated the data group by state. In the third state in sorts the states according to the difference between their hottest month and coldest month. Detailed Project Description can be in project description.docx file in the repo.

Authors: 

Mohiuddin Abdul Kader: mabdu002@cs.ucr.edu
Pritom Ahmed: pahme002@cs.ucr.edu

Sample output can be found in sample output.xlsx file.     

              
The project can be divided into two parts. 

First where we take the two input files and join them to get all the information required for our calculations. Here we find the common information between the two input data which is Station ID and join the two data using that. For this project we only a very limited set of information from the datasets. So we only take those informations and discard the others. To summarize, we actually get the state code from the input which contain the station location information and we create one entry per station which contains the state code and the average temperature per month for that station. 

In the second part, we aggregate the data mentioned above by state so that there is only one entry per state. For each state we find the month with highest and lowest average temperature, the temperature themselves and difference between the two months with highest and lowest average temperature. So each entry has state code, the month with highest average temperature, the temperature itself, the month with lowest average temperature and the temperature itself. As the output is not sorted, we sorted them based on the difference between the highest and lowest averages by using another mapreduce job using the difference as key.

We divided the problem into 3 map reduce jobs. 

In the first job, we wrote two mapper class for two different datasets. These mappers output station id as key and the useful information from the datasets as value. We only considered the data for US. The reducer then joined the two datasets and here we perform a aggregation on each station information. We obtain average temperature recorded for each month for a station. Finally, before writing the output value to a temporary output file we append the corresponding state of that particular Station ID. After the job, we get a file where for each station, the average of the temperature readings for each of the 12 months with State code (which is  also the input file for the second job). We measured the time taken to run this job. This job takes 35 seconds.

In the second job, takes the intermediate output file of the first job as its input file. Here, we calculate the months with the highest and lowest averages for each state. The Mapper class for the job, maps using state code as key and the average temperature for each month as value. In the Reducer class, we aggregate all the data of individual stations for a particular state and calculate the months with the highest and lowest average for each state and the highest and lowest average temperatures itself. This job takes 10 seconds.

In the third job, we actually sort the difference between highest and lowest temperatures of various state in ascending order. The mapper class uses the difference between highest and lowest average temperatures as key. As a result, the output gets sorted by their difference after the Reducer is done with it. This job takes 9 seconds. 
The two Mapper classes for the first job one for each type of inputs, takes the data from the WeatherStationLocations.csv file which contains the stations’ location information and the station readings for the year 2006, 2007, 2008 and 2009. They have only station ID common between them as “USAF”/”STN---”. So naturally we chose Station ID as the key to join the two input data. 

For extra credit, we have included the calculation for precipitation in our project. We used the letter at the end of the recording to adjust the readings accordingly by multiplying in order to get the 24 hour readings. We have used letters from “A” to “G” and considered “H” and “I” as 0.0. We considered 99.99 as 0 for all the records. We aggregated all the records from a state the same way we calculated for the temperature. 
