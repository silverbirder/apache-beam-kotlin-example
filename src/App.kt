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
}