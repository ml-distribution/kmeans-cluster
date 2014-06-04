HW7: K-Means Clustering on Hadoop
=================================

This is a colleciton of Java files using the Hadoop framework to build a distributed version of K-Means clustering. It consists of three MR tasks: the first two read in the data and the initial centroids respectively, converting them to the proper data formats. The last is the clustering step, which is run iteratively until convergence.

To create the .jar archive:

    javac *.java
    jar cfm KMeans.jar Manifest.txt com/

*By specifying some `Manifest.txt` file, you can point Java--and hence, Hadoop--to the class with the `main` method. Just include the following line (followed by a newline) in the text file:*

    Main-Class: Package.ClassName

To run the job:

    hadoop jar KMeans.jar -D input=/path/to/data/instances/ -D centroids=/path/to/initial/centroids/ -D output=/output/dir/ [-D reducers=10] [-D n_clusters=8] [-D tolerance=1e-6]
    
