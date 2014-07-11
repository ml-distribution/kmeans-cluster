

*** 本项目涉及三个MapReduce作业
1. 第一个作业用于读取原始数据；
2. 第二个作业用于初始化聚类中心，转换成合适的数据格式；
3. 第三个作业是聚类操作，循环迭代，直到收敛。


```java
运行命令：

hadoop jar kmeans-cluster-jar-with-dependencies.jar kMeansClusterDistribute -D input=input-data -D output=output-data -D centroids=centroids-data [-D clusters=5] [-D reducers=10] [-D tolerance=0.000001]
```