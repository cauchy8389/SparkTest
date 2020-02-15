// src/main/scala/progscala3/forcomps/LoginFormValidatorSingle.scala

package progscala2.forcomps

object LoginFormValidatorSingle {                                    // <1>

  type E[T] = Either[LoginValidation, T]  // shorter

  def nonEmpty(field: String, name: String): E[String] =
    Either.cond(
      field.length > 0,
      field,
      Empty(name))

  def notTooShort(field: String, name: String, n: Int): E[String] =
    Either.cond(
      field.length >= n,
      field,
      TooShort(name, n))

  /** For simplicity, just disallow whitespace. */
  def goodCharacters(field: String, name: String): E[String] = {
    val re = raw".*\s.*".r
    Either.cond(
      re.pattern.matcher(field).matches == false,
      field,
      BadCharacters(name))
  }

  def apply(userName: String, password: String): E[ValidLoginForm] = // <2>
    for {
      _ <- nonEmpty(userName, "user name").right
      _ <- notTooShort(userName, "user name", 5).right
      _ <- goodCharacters(userName, "user name").right
      _ <- nonEmpty(password, "password").right
      _ <- notTooShort(password, "password", 5).right
      _ <- goodCharacters(password, "password").right
    } yield ValidLoginForm(userName, password)

  def main(args: Array[String]): Unit = {
    assert(LoginFormValidatorSingle("", "pwd") ==
      Left(Empty("user name")))
    assert(LoginFormValidatorSingle("", "") ==
      Left(Empty("user name")))                                      // <3>
    assert(LoginFormValidatorSingle("12", "") ==
      Left(TooShort("user name", 5)))
    assert(LoginFormValidatorSingle("12", "67") ==
      Left(TooShort("user name", 5)))

    assert(LoginFormValidatorSingle("12345", "67") ==
      Left(TooShort("password", 5)))
    assert(LoginFormValidatorSingle("123 45", "67") ==
      Left(BadCharacters("user name")))
    assert(LoginFormValidatorSingle("12345", "678 90") ==
      Left(BadCharacters("password")))

    assert(LoginFormValidatorSingle("12345", "67890") ==
      Right(ValidLoginForm("12345", "67890")))
  }
}
