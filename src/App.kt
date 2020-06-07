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

    val ary = arrayOf(1,2,3)
    var list = arrayListOf(1,2,3)
    var mList = mutableListOf(1,2,3)
    var map = mapOf("a" to "b")
    var mMap = mutableMapOf("a" to "b")
    var set = setOf(1, 2, 3)
    // list: 順序持ち. 重複可
    // Set: 順序持たない。重複不可
    // Map: キーバリュー.
    // list,set,mapはreadonly
}