// src/main/scala/progscala3/basicoop/people/Zipcode.sc
import progscala2.basicoop.people.ZipCode

println(ZipCode("12345"))
// Result: ZipCode = 12345

println(ZipCode("12345", "6789"))
// Result: ZipCode = 12345-6789

println(ZipCode("12345", ""))
// Result: ZipCode = 12345

Seq("0", "1", "12", "123", "1234", "123456",
  "1234e", "123d5",  "12c45", "1b345", "a2345") foreach { z =>
  try {
    ZipCode(z, "6789")
  } catch {
    case e: java.lang.IllegalArgumentException => println(s"Expected bad: $z")
  }
  try {
    ZipCode(z)
  } catch {
    case _: java.lang.IllegalArgumentException => println(s"Expected bad: $z")
  }
}

Seq("1", "12", "123", "12345",
  "123d",  "12c4", "1b34", "a234") foreach { e =>
  try {
    ZipCode("12345", e)  // Invalid Zip+4 specified, e.g., 12345-0
  } catch {
    case _: java.lang.IllegalArgumentException => println(s"Expected bad: $e")
  }
}
