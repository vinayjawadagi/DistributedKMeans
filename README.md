# Hadoop MapReduce Distributed KMeans Algorithm
Hadoop MapReduce WordCount Demo

## Installation

These components need to be installed first:

- OpenJDK 11
- Hadoop 3.3.5
- Maven (Tested with version 3.6.3)
- AWS CLI (Tested with version 1.22.34)

After downloading the hadoop installation, move it to an appropriate directory:

`mv hadoop-3.3.5 /usr/local/hadoop-3.3.5`

## Environment

1. Example ~/.bash_aliases:

   ```
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   export HADOOP_HOME=/usr/local/hadoop-3.3.5
   export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
   ```

2. Explicitly set `JAVA_HOME` in `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:

   `export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`

## Execution

All of the build & execution commands are organized in the Makefile.

1. Unzip project file.
2. Open command prompt.
3. Navigate to directory where project files unzipped.
4. Edit the Makefile to customize the environment at the top.
   Sufficient for standalone: hadoop.root, jar.name, local.input
   Other defaults acceptable for running standalone.
5. 1st parallelization (parallelize single k clustering, but run clustering for different k's sequentially), aws run: - In the `Makefile`, uncomment line `job.name=kmeans.KmeansClustering` and comment out line `job.name=kmeans.KmeansParallelK` - Add a folder inside project called `input` and put the following processed data input file into `input`: https://northeastern-my.sharepoint.com/personal/jawadagi_v_northeastern_edu/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fjawadagi%5Fv%5Fnortheastern%5Fedu%2FDocuments%2Fprocessed%2Dkmeans%2Dinput&parent=%2Fpersonal%2Fjawadagi%5Fv%5Fnortheastern%5Fedu%2FDocuments&ga=1
   - `make aws k={k} maxIterations={maxIterations}`
     - For example, to run k = 2 with maxIteration set to 125, run `make aws k=2 maxIterations=125`
6. 2nd parallelization (parallelize running multiple clustering for different k and other parameters (like distance calculation method), but each clustering runs sequentially), local run: - - In the `Makefile`, uncomment line `job.name=kmeans.KmeansParallelK` and comment out line `job.name=kmeans.KmeansClustering` - Add a folder inside project called `input`. Inside this `input` folder, add 3 folders: `init`, `input`, `kmeans`. Put the processed data input file into `init`: https://northeastern-my.sharepoint.com/personal/jawadagi_v_northeastern_edu/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fjawadagi%5Fv%5Fnortheastern%5Fedu%2FDocuments%2Fprocessed%2Dkmeans%2Dinput&parent=%2Fpersonal%2Fjawadagi%5Fv%5Fnortheastern%5Fedu%2FDocuments&ga=1 - `make local` - In the `run()` method for `KMeansParallelK` class, change the `n={n_value}`. This controls the number of random parameters used to run the program. For example, for n = 5, the program will generate 5 sets of parameters, k ranging from 2 to 6, and distance measurement either Euclidean or Manhattan. The program will run n K Means Clustering with these sets of parameters in parallel.
