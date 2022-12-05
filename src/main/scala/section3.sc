// RDD(Resillient Distributed Data)

/*

text File을 읽고 MAP OPERATION을 통해 분배하고, count by value를 통해 합친다.

key/value RDD

Key : age
Value : friends

totalByAge = rdd.map(x => (x,1)) # key = x, value = 1

1. reduceByKey
특정 키로 어떻게 값을 결합하는지 정의.
((X,Y) => X + Y )

2. groupByKey
특정 개인 key에 모든 value의 집단을 return 한다.

3. sortByKey


mapping operation을 하다 보면 value 부분만 변경하는 경우가 다수 일 것이다.
mapValues, flatMapValues 를 쓰면 좋다.

.reduceByKey



 */

