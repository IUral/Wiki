# Wikipedia
## _Compute a ranking of programming languages_

[![N|Solid](https://spark.apache.org/images/spark-stack.png)](https://spark.apache.org/)

### 1. You need to download the data (133 MB): https://moocs.scala-lang.org/~dockermoocs/bigdata/wikipedia.dat

### 2. You need wikipedia.dat file place it in the folder : `src/main/resources/wikipedia` in your project directory.

### 3. Installing the JDK - 8
[![Build Status](https://www.portalprogramas.com/imagenes/programas/es/693/14693_1.media.jpg)](https://www.oracle.com/ru/java/technologies/javase/javase-jdk8-downloads.html)

### 4. Installing sbt - 1.4.7
[![Build Status](https://www.scala-sbt.org/release/docs/files/sbt-logo.svg)](https://www.scala-sbt.org/release/docs/Setup.html)

### 5. Open Intellij IDEA and Install Scala plugin
[![Build Status](https://resources.jetbrains.com/help/img/idea/2020.3/scala_plugin_page.png)]

### 6. Select Import Project and open the build.sbt file for your project

### 7. **Use sbt.version=1.4.7**

### 8. Run WikipediaRanking

### 9. Go to SparkUI 
```sh
http://localhost:4040/
```
 -----------------
 # Out:
|       lang|count|
|-----------|-----|
| JavaScript| 1692|
|         C#|  705|
|       Java|  586|
|        CSS|  372|
|        C++|  334|
|     MATLAB|  295|
|     Python|  286|
|        PHP|  279|
|       Perl|  144|
|       Ruby|  120|
|    Haskell|   54|
|Objective-C|   47|
|      Scala|   43|
|    Clojure|   26|
|     Groovy|   23|


**Processing Part 1: naive ranking `${stop - start}` ms.**  
**Processing Part 2: ranking using inverted index took `${stop - start}`  ms.**  
**Processing Part 3: ranking using reduceByKey took `${stop - start}` ms.**
