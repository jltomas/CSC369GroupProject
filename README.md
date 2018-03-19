# Team: git reset --hard HEAD
## Ben Whittle, Justin Tomas, Kort Heyneman

Dataset:
https://www.kaggle.com/mchirico/montcoalert

# Results:

Our analysis disproved some of our predictions, confirmed others, and revealed interesting new insights we had not set out to find.
By examining the timing, volume, and location of emergencies, we observed some interesting patterns of human behavior. 

# Hadoop

* Hadoop Spark was very useful in helping us analyze our data set. 
* The dataset contains all of the 911 call information from several years worth of data, so processing it linearly was quite slow. 
* Analyzing and writing the data took 6 seconds on the cluster, while locally it generally took over 30s
* Spark also creates a great framework for this project to be expanded. If the dataset was widened to include the 911 calls from every county in the nation, the size of the data would quickly become impossible to handle on a single machine. 
* The speedup observed in the processing of our limited sample proves that Spark is perfect for large scale analysis of emergency data.

## Analytics
* Analysis of emergencies based on location (Justin)
* Analysis of overall emergency trends (Ben)
* Analysis of most common emergencies (Ben)
* Analysis of emergencies based on time (Kort)
