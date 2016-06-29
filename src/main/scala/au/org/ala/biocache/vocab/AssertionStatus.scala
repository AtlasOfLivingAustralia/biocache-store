package au.org.ala.biocache.vocab

/**
 * Simple enum of quality assertion code status.
 */
object AssertionStatus {

  // For System Assertions
  val FAILED = 0
  val PASSED = 1
  val UNCHECKED = 2

  // For user assertions
  val QA_OPEN_ISSUE = 50001
  val QA_VERIFIED = 50002
  val QA_CORRECTED = 50003
  val QA_NONE = 50004
  val QA_UNCONFIRMED = 50005

  def isUserAssertionType(code:Int) :Boolean = {
    (code == QA_OPEN_ISSUE || code == QA_UNCONFIRMED || code == QA_NONE || code == QA_VERIFIED)
  }

  def getQAAssertionName (code: String) : String = {
    Integer.parseInt(code) match {
      case QA_OPEN_ISSUE => return "QA_OPEN_ISSUE"
      case QA_VERIFIED => return "QA_VERIFIED"
      case QA_CORRECTED => return "QA_CORRECTED"
      case QA_NONE => return "QA_NONE"
      case QA_UNCONFIRMED => return "QA_UNCONFIRMED"
      case _ => return code.toString
    }
  }

}
