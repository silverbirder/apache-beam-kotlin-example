import io.kotest.core.spec.style.StringSpec
import io.kotest.inspectors.forExactly
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldStartWith

class MyTests : StringSpec({
    "your test case" {
        val xs = listOf("aa_1", "aa_2", "aa_3")
        xs.forExactly(3) {
            it.shouldContain("_")
            it.shouldStartWith("aa")
        }
    }
})