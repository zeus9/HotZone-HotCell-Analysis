# HotZone-HotCell-Analysis
Perform Hot zone and Hotel Cell Spatial queries in Apache Spark on the New York Taxi Dataset.

### Hot zone analysis
We perform a range join operation on a rectangle datasets and a point dataset. For each rectangle, the number of points located within the rectangle will be obtained. The hotter rectangle means that it include more points. So this operation is to calculate the hotness of all the rectangles. 

### Hot cell analysis

This task focuses on applying spatial statistics to spatio-temporal big data in order to identify statistically significant spatial hot spots using Apache Spark. The topic of this task is from ACM SIGSPATIAL GISCUP 2016.

The Problem Definition page is here: [http://sigspatial2016.sigspatial.org/giscup2016/problem](http://sigspatial2016.sigspatial.org/giscup2016/problem) 

The Submit Format page is here: [http://sigspatial2016.sigspatial.org/giscup2016/submit](http://sigspatial2016.sigspatial.org/giscup2016/submit)

Note that: You may clip the source data to an envelope (latitude 40.5N – 40.9N, longitude 73.7W – 74.25W) encompassing the New York City in order to remove some of the noisy error data.

We mplement a Spark program to calculate the Getis-Ord statistic of NYC Taxi Trip datasets. We call it "**Hot cell analysis**"


