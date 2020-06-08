fun sum(a: Int, b: Int): Int {
    return a + b
}

fun describe(obj: Any): String =
    when (obj) {
        1 -> "One"
        "Hello" -> "Greeting"
        else -> "Unknown"
    }

data class Customer(val name: String = "default", val email: String)

object Resource {
    val name = "Name"
}

class Turtle {
    fun penDown() {
        println("penDown")
    }
    fun penUp() {
        println("penUp")
    }
}

// @see https://dogwood008.github.io/kotlin-web-site-ja/docs/reference/
fun main() {
    val fruits = listOf("banana", "avocado", "apple", "kiwifruit")
    fruits
        .filter { it.startsWith("b") }
        .sortedBy { it }
        .map { it.toUpperCase() }
        .forEach { println(it) }
    var c = Customer(email = "name@email.com")

    val lazyValue: String by lazy {
        println("computed!")
        "Hello"
    }
    println(lazyValue)
    println(lazyValue)

    val ary = arrayOf(1, 2, 3)
    var list = arrayListOf(1, 2, 3)
    var mList = mutableListOf(1, 2, 3)
    var map = mapOf("a" to "b")
    var mMap = mutableMapOf("a" to "b")
    var set = setOf(1, 2, 3)

    // @see https://qiita.com/opengl-8080/items/4d335dafe526dd17d96e
    // list: 順序持ち. 重複可
    // Set: 順序持たない。重複不可
    // Map: キーバリュー.
    // list,set,mapはreadonly

    val fil = ary.filter { it > 0 }

    fun theAnswer() = 42
    println(theAnswer())
    val myTurtle = Turtle()
    with(myTurtle) {
        penDown()
        penUp()
    }

    fun arrayOfMinusOnes(size: Int): IntArray {
        return IntArray(size).apply { fill(-1) }
    }
    // @see https://qiita.com/AAkira/items/16ae2e9c0f6073e0e983
    // Unit is void
    println(arrayOfMinusOnes(1))
}