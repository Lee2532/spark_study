val hello: String = "Hala" // 불변상수

var helloThere: String = hello

helloThere = hello + " There!"

println(helloThere)


for (i:Int <- 0 to 5) {
  println(i)
}

def example(a: Int): Unit = {
  a match {
    case 1 => println("1")
    case 2 => println("2")
    case 3 => println("3")
    case _ => println("other")
  }
}
example(3)



"a" match{
  case "a" => println("123123")
  case _ => println("asdasd")
}


// Tuple
val captaionStuff = ("picard", "Enterprise-D", "NCC-1701-D")
println(captaionStuff)


println(captaionStuff._1)
println(captaionStuff._2)
println(captaionStuff._3)

val picardsShop = "Picard" -> "Enterpricse-D"
println(picardsShop._2)

// 서로 다른 타입을 가져도 된다
val aBunchOfStuff = ("Kirk", 1964, true)

// List
// 강의에서는 리스트에 서로 다른 타입을 가질 수 없다고 하지만
// 타입이 다른게 생길경우 Any 타입으로 지정


val shipList = List("Enterprise", "Defiant", "voyager", "Deep sadsd")

//val shipList = List("Enterprise", "Defiant", "voyager", true)


println(shipList(1))
// 튜플과는 다르게 0 부터 시작
println(shipList(0))

println(shipList.head)
//첫 항목을 제외하고 출력
println(shipList.tail)

for (ship <- shipList) {
  println(ship)
}
// map
val backwardShip = shipList.map( (ship : String) => {ship.reverse})
for (ship <- backwardShip) { println(ship)}


// reduce

val numberList = List(1, 2, 3, 4, 5)
val sum = numberList.reduce( (x: Int , y: Int) => x + y)
println(sum)

// filter() remove stuff

val iHateFives = numberList.filter( (x: Int) => x != 5)
println(iHateFives)

val iHateThrees = numberList.filter(_ != 3)
println(iHateThrees)

val moreNumbers = List(6, 7, 8)
// 리스트를 연결할때는 더블 플러스 ++ 를 사용
val lostOfNumbers = numberList ++ moreNumbers

val reversed = numberList.reverse
println(reversed)

val sorted = reversed.sorted

val lotsOfDuplicates = numberList ++ numberList


val distinctValues = lotsOfDuplicates.distinct

val maxValue = numberList.max

val toatal = numberList.sum

val hasTree = iHateThrees.contains(3)

// MAPS

val shipMap = Map("Kirk" -> "Enterprise", "Picard" -> "Enterprise", "Sisko" -> "Deep Space Nine", "Janeway" -> "Voyager")

println(shipMap("Janeway"))
println(shipMap.contains("Archer"))

val archerShip = util.Try(shipMap("Archer")) getOrElse("Unknown")
println(archerShip)

// 과제
// 숫자를 1부터 20까지의 리스트를 만들고 3으로 균등하게 나눌 수 있는 숫자들을 출력하기

val numList = List(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)

numList.filter( _ %3 == 0)