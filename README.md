# spark_study

강의 자료 

https://www.udemy.com/course/best-scala-apache-spark/


## spark history server

### http://localhost:18080/

spark-class.cmd org.apache.spark.deploy.history.HistoryServer


### github Table
```
+---+-----------+---------+--------+-------------+-------------------+---------+--------------------+----------+----+------+-------------------+
|idx|         id|     type|actor_id|  actor_login|actor_display_login|  repo_id|           repo_name|   push_id|size|public|         created_at|
+---+-----------+---------+--------+-------------+-------------------+---------+--------------------+----------+----+------+-------------------+
|  1|11185281241|PushEvent|16885693|    briangu33|          briangu33|230988632|     briangu33/Setup|4450610249|   3|  true|2020-01-01 09:00:00|
|  2|11185281242|PushEvent|33404015|   MarcoBozic|         MarcoBozic|230689169|MarcoBozic/JokesW...|4450610250|   1|  true|2020-01-01 09:00:00|
|  3|11185281247|PushEvent|58833107|   ripamf2991|         ripamf2991|227725053|    ripamf2991/ntdtv|4450610253|   1|  true|2020-01-01 09:00:00|
|  4|11185281248|PushEvent|58833107|   ripamf2991|         ripamf2991|227724995|      ripamf2991/djy|4450610252|   1|  true|2020-01-01 09:00:00|
|  5|11185281253|PushEvent|  852234|    formigone|          formigone| 77938842| formigone/portfolio|4450610257|   1|  true|2020-01-01 09:00:00|
|  6|11185281255|PushEvent| 4382547|     aritsune|           aritsune|151743765|massif-press/compcon|4450610258|   1|  true|2020-01-01 09:00:00|
|  7|11185281256|PushEvent|22035876|mikeshardmind|      mikeshardmind|116521735|mikeshardmind/Red...|4450610259|   2|  true|2020-01-01 09:00:00|
|  8|11185281259|PushEvent|15944199|  nekonomicon|        nekonomicon| 52468951|   FWGS/hlsdk-xash3d|4450610261|  12|  true|2020-01-01 09:00:00|
|  9|11185281277|PushEvent|38684511|      faraz-b|            faraz-b|204365549|     sfuphantom/BSPD|4450610270|   1|  true|2020-01-01 09:00:01|
| 10|11185281290|PushEvent|49124992|  ELAMRANI744|        ELAMRANI744|230340142|ELAMRANI744/Datab...|4450610276|   1|  true|2020-01-01 09:00:02|
| 11|11185281295|PushEvent| 3991134|   allenyllee|         allenyllee|113654558|      allenyllee/ode|4450610277|   0|  true|2020-01-01 09:00:02|
| 12|11185281299|PushEvent|31647663|    Driverorz|          Driverorz|230852637| Driverorz/My-Action|4450610281|   1|  true|2020-01-01 09:00:02|
| 13|11185281300|PushEvent|37038626|       geos4s|             geos4s|159298221|    geos4s/18w856162|4450610282|   1|  true|2020-01-01 09:00:02|
| 14|11185281301|PushEvent|58833107|   ripamf2991|         ripamf2991|227725053|    ripamf2991/ntdtv|4450610278|   1|  true|2020-01-01 09:00:02|
| 15|11185281304|PushEvent| 8517910|    LombiqBot|          LombiqBot| 43380114|Lombiq/Smart-Noti...|4450610284|   0|  true|2020-01-01 09:00:02|
| 16|11185281309|PushEvent| 4581915|  lucwastiaux|        lucwastiaux|140990879|lucwastiaux/AnkiR...|4450610288|   1|  true|2020-01-01 09:00:02|
| 17|11185281321|PushEvent|57423851|   netboot-ci|         netboot-ci| 41636424|netbootxyz/netboo...|4450610290|   1|  true|2020-01-01 09:00:02|
| 18|11185281324|PushEvent|56168522|       binyaz|             binyaz|213518575|binyaz/for-loops-...|4450610293|   1|  true|2020-01-01 09:00:03|
| 19|11185281333|PushEvent|42398321|blcksm1th1992|      blcksm1th1992|228754057|blcksm1th1992/Scr...|4450610296|   1|  true|2020-01-01 09:00:03|
| 20|11185281348|PushEvent|58833107|   ripamf2991|         ripamf2991|227725053|    ripamf2991/ntdtv|4450610308|   1|  true|2020-01-01 09:00:03|
+---+-----------+---------+--------+-------------+-------------------+---------+--------------------+----------+----+------+-------------------+

```