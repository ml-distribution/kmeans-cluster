K-Means Clustering的基本实现及其分布式实现
=================================

To run the job:

    hadoop jar KMeans.jar -D input=/path/to/data/instances/ -D centroids=/path/to/initial/centroids/ -D output=/output/dir/ [-D reducers=10] [-D n_clusters=8] [-D tolerance=1e-6]
    
