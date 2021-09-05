# Spark Optimization 
The purpose of this project is to optimize the original query. Various methods were tried: loading selected columns, braodcast, changing join order, cache and the spark UI hosted at http://LAPTOP-TN9MOJSU.mshome.net:4040 was used to analyze the time and resources for each computation. 

## Original Query

The original script reads questions and answers from parquet files in a dictionary and organizes them into the following df: 

Answers: 
```python 
+-----------+---------+--------------------+--------+-------+-----+
|question_id|answer_id|       creation_date|comments|user_id|score|
+-----------+---------+--------------------+--------+-------+-----+
|     226592|   226595|2015-12-29 17:46:...|       3|  82798|    2|
|     388057|   388062|2018-02-22 12:52:...|       8|    520|   21|
|     293286|   293305|2016-11-17 15:35:...|       0|  47472|    2|
|     442499|   442503|2018-11-22 00:34:...|       0| 137289|    0|
|     293009|   293031|2016-11-16 07:36:...|       0|  83721|    0|
|     395532|   395537|2018-03-25 00:51:...|       0|   1325|    0|
|     329826|   329843|2017-04-29 10:42:...|       4|    520|    1|
|     294710|   295061|2016-11-26 19:29:...|       2| 114696|    2|
|     291910|   291917|2016-11-10 04:56:...|       0| 114696|    2|
|     372382|   372394|2017-12-03 20:17:...|       0| 172328|    0|
|     178387|   178394|2015-04-25 12:31:...|       6|  62726|    0|
|     393947|   393948|2018-03-17 17:22:...|       0| 165299|    9|
|     432001|   432696|2018-10-05 03:47:...|       1| 102218|    0|
|     322740|   322746|2017-03-31 13:10:...|       0|    392|    0|
|     397003|   397008|2018-04-01 06:31:...|       1| 189394|    6|
|     223572|   223628|2015-12-11 23:40:...|       0|  94772|   -1|
|     220328|   220331|2015-11-24 09:57:...|       3|  92883|    1|
|     176400|   176491|2015-04-16 08:13:...|       0|  40330|    0|
|     265167|   265179|2016-06-28 06:58:...|       0|  46790|    0|
|     309103|   309105|2017-02-01 11:00:...|       2|  89597|    2|
+-----------+---------+--------------------+--------+-------+-----+
```
Questions: 

```python 
+-----------+--------------------+--------------------+--------------------+------------------+--------+-------+-----+
|question_id|                tags|       creation_date|               title|accepted_answer_id|comments|user_id|views|
+-----------+--------------------+--------------------+--------------------+------------------+--------+-------+-----+
|     382738|[optics, waves, f...|2018-01-27 23:22:...|What is the pseud...|            382772|       0|  76347|   32|
|     370717|[field-theory, de...|2017-11-25 01:09:...|What is the defin...|              null|       1|  75085|   82|
|     339944|[general-relativi...|2017-06-17 13:32:...|Could gravitation...|              null|      13| 116137|  333|
|     233852|[homework-and-exe...|2016-02-04 13:19:...|When does travell...|              null|       9|  95831|  185|
|     294165|[quantum-mechanic...|2016-11-22 03:39:...|Time-dependent qu...|              null|       1| 118807|   56|
|     173819|[homework-and-exe...|2015-04-02 08:56:...|Finding Magnetic ...|              null|       5|  76767| 3709|
|     265198|    [thermodynamics]|2016-06-28 07:56:...|Physical meaning ...|              null|       2|  65035| 1211|
|     175015|[quantum-mechanic...|2015-04-08 18:24:...|Understanding a m...|              null|       1|  76155|  326|
|     413973|[quantum-mechanic...|2018-06-27 06:29:...|Incorporate spino...|              null|       3| 167682|   81|
|     303670|[quantum-field-th...|2017-01-07 22:05:...|A Wilson line pro...|              null|       0| 101968|  184|
|     317368|[general-relativi...|2017-03-08 11:53:...|Shouldn't Torsion...|              null|       0|  20427|  305|
|     369982|[quantum-mechanic...|2017-11-20 19:11:...|Incompressible in...|              null|       4| 124864|   83|
|     239745|[quantum-mechanic...|2016-02-25 00:51:...|Is this correct? ...|            239773|       2|  89821|   78|
|     412294|[quantum-mechanic...|2018-06-17 17:46:...|Is electron/photo...|              null|       0|    605|   61|
|     437521|[thermodynamics, ...|2018-10-28 23:49:...|Distance Dependen...|              null|       2| 211152|   19|
|     289701|[quantum-field-th...|2016-10-29 20:56:...|Generalize QFT wi...|              null|       4|  31922|   49|
|     239505|[definition, stab...|2016-02-24 02:51:...|conditions for so...|              null|       3| 102021|  121|
|     300744|[electromagnetism...|2016-12-24 10:14:...|Maxwell equations...|            300749|       0| 112190|  171|
|     217315|[nuclear-physics,...|2015-11-08 01:13:...|Is the direction ...|              null|       1|  60150| 1749|
|     334778|[cosmology, cosmo...|2017-05-22 06:58:...|Why are fluctatio...|            334791|       3| 109312|  110|
+-----------+--------------------+--------------------+--------------------+------------------+--------+-------+-----+
```
Then number of answers per question per month is aggregated counted and joined to the questionsDF on question id. 
The original query executed in 9s. 

## Changing Join Order 
Since join operations are expensive, one of the ways to 
```python
resultDF = answers_month.join(questionsDF, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')
```
Changing join order does not improve the performance in this case - 9s. The reason is the sizes of two tables are similar and thus there is no benefit to changing the join order around. 

## Loading Fewer Columns to DF 
```python
questionsDF = spark.read.parquet('path', questions_input_path).select('question_id','creation_date','title')
answersDF = spark.read.parquet('path', answers_input_path).select('question_id','answer_id','creation_date')
```
The idea for this one was to selectively read columns from the parquet file instead of the entire dataset to reduce the size of the df for future join operations. This did not significantly improve performance - 7.8s. 

## Broadcast 
```python
resultDF = questionsDF.join(broadcast(answers_month), 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')
```
Broadcast barely improved performance - 8s. The reason is we use standalone server with only two worker nodes and the dataset is not large enough to make the broadcast works effectively.

## Cache 
```python 
answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

answers_month.cache()
# resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF = questionsDF.join(broadcast(answers_month), 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()
```
Caching dataframe improve the performance significantly - processing in about 5s. While cache does not always improve performance, in this case cache works great because the dataset is small. 
