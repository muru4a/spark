
import org.apache.spark.sql.functions._

object customudfs {

  private val AgeBucket18_25 = "18-25"
  private val AgeBucket25_30 = "25-30"
  private val AgeBucket30_35 = "30-35"
  private val AgeBucket35_40 = "35-40"
  private val AgeBucket40_45 = "40-45"
  private val AgeBucket45_50 = "45-50"
  private val AgeBucket50_55 = "50-55"
  private val AgeBucket55_60 = "55-60"
  private val AgeBucket60_65 = "60-65"
  private val AgeBucket65_70 = "65-70"
  private val AgeBucket70_100 = "70-100"
  private val AgeBucket26_35 = "26-35"
  private val AgeBucket36_50 = "36-50"
  private val AgeBucket51_60 = "51-60"
  private val AgeBucket60Plus = "60+"
  private val UnknownBucket = "Unknown"
  private val IncomeBucket15K = "< $15,000"
  private val IncomeBucket15K_30K = "$15,000 - $29,999"
  private val IncomeBucket30K_50K = "$30,000 - $49,999"
  private val IncomeBucket50K_75K = "$50,000 - $74,999"
  private val IncomeBucket75K_100K = "$75,000 - $99,999"
  private val IncomeBucket100KPlus = "> $100,000"
  private val GenderCodeMale = "M"
  private val GenderCodeFemale = "F"
  private val GenderCodeUnknown = "U"
  private val GenderDescMale = "Male"
  private val GenderDescFemale = "Female"
  private val GenderDescUnknown = "Unknown"
  private val IncomeRangeCodeOne = "1"
  private val IncomeRangeCodeTwo = "2"
  private val IncomeRangeCodeThree = "3"
  private val IncomeRangeCodeFour = "4"
  private val IncomeRangeCodeFive = "5"
  private val IncomeRangeCodeSix = "6"
  private val IncomeRangeCodeUnknown = "-99"
  private val AgeBucketCode1 = 1
  private val AgeBucketCode2 = 2
  private val AgeBucketCode3 = 3
  private val AgeBucketCode4 = 4
  private val AgeBucketCode5 = 5
  private val AgeBucketCodeUnknown = -99
  private val StateNotAvailable = "Not Available"

  /*
   * UDFs for DE demographic dimensions
   */

  /*
   *  UDF for cust_gender_code
   *  Use gender column from acxiom table to decide gender for customer
   */
  val getCustGenderCodeDe = udf((genderCol: String) => {
    genderCol match {
      case "1" => GenderCodeMale
      case "2" => GenderCodeFemale
      case _ => GenderCodeUnknown
    }
  })

  /*
   *  UDF for cust_gender_desc
   *  Use gender column from acxiom table to decide gender for customer
   */
  val getCustGenderDescriptionDe = udf((genderCol: String) => {

    genderCol match {
      case "1" => GenderDescMale
      case "2" => GenderDescFemale
      case _ => UnknownBucket
    }
  })

  /*
   *  UDF for cust_age_grp_code
   *  Use age column from acxiom table to decide age group for customer
   */
  val getCustAgeGrpCodeDe = udf((ageBucket: String) => {

    ageBucket match {
      case AgeBucket18_25 => AgeBucketCode1
      case AgeBucket25_30 | AgeBucket30_35 => AgeBucketCode2
      case AgeBucket35_40 | AgeBucket40_45 | AgeBucket45_50 => AgeBucketCode3
      case AgeBucket50_55 | AgeBucket55_60 => AgeBucketCode4
      case AgeBucket60_65 | AgeBucket65_70 | AgeBucket70_100 => AgeBucketCode5
      case _ => AgeBucketCodeUnknown
    }
  })

  /*
   *  UDF for cust_age_desc
   *  Use age column from acxiom table to decide age group for customer
   */
  val getCustAgeGrpDescDe = udf((ageBucket: String) => {

    ageBucket match {
      case AgeBucket18_25 => AgeBucket18_25
      case AgeBucket25_30 | AgeBucket30_35 => AgeBucket26_35
      case AgeBucket35_40 | AgeBucket40_45 | AgeBucket45_50 => AgeBucket36_50
      case AgeBucket50_55 | AgeBucket55_60 => AgeBucket51_60
      case AgeBucket60_65 | AgeBucket65_70 | AgeBucket70_100 => AgeBucket60Plus
      case _ => UnknownBucket
    }
  })

  /*
   * UDFs for UK and Au demographic dimensions
   */

  /*
   *  UDF for cust_gender_code
   *  Use gender column from acxiom table to decide gender for customer
   */
  val getCustGenderCodeUkAu = udf((genderCode: String) => {
    genderCode match {
      case GenderCodeMale => GenderCodeMale
      case GenderCodeFemale => GenderCodeFemale
      case _ => GenderCodeUnknown
    }
  })

  /*
   *  UDF for cust_gender_desc
   *  Use gender column from acxiom table to decide gender for customer
   */
  val getCustGenderDescriptionUkAu = udf((genderCode: String) => {

    genderCode match {
      case GenderCodeMale => GenderDescMale
      case GenderCodeFemale => GenderDescFemale
      case _ => GenderDescUnknown
    }
  })

  /*
   *  UDF for cust_age_grp_code
   *  Use age column from acxiom table to decide age group for customer
   */
  val getCustAgeGrpCodeUkAu = udf((ageBucket: String) => {

    ageBucket match {
      case "01" => AgeBucketCode1
      case "02" | "03" => AgeBucketCode2
      case "04" | "05" | "06" => AgeBucketCode3
      case "07" | "08" => AgeBucketCode4
      case "09" | "10" | "11" | "12" => AgeBucketCode5
      case _ => AgeBucketCodeUnknown
    }
  })

  /*
   *  UDF for cust_age_grp_desc
   *  Use age column from acxiom table to decide age group for customer
   */
  val getCustAgeGrpDescUkAu = udf((ageBucket: String) => {

    ageBucket match {
      case "01" => AgeBucket18_25
      case "02" | "03" => AgeBucket26_35
      case "04" | "05" | "06" => AgeBucket36_50
      case "07" | "08" => AgeBucket51_60
      case "09" | "10" | "11" | "12" => AgeBucket60Plus
      case _ => UnknownBucket
    }
  })

  /*
   *  UDF for cust_est_income_range_code
   *  Use age column from acxiom table to decide income bucket for customer
   */
  val getCustEstIncomeRangeCodeUkAu = udf((incomeBucket: String) => {

    incomeBucket match {
      case "1" | "2" => IncomeRangeCodeOne
      case "3" | "4" => IncomeRangeCodeTwo
      case "5" | "6" => IncomeRangeCodeThree
      case "7" | "8" => IncomeRangeCodeFour
      case "9" => IncomeRangeCodeFive
      case "10" => IncomeRangeCodeSix
      case _ => IncomeRangeCodeUnknown
    }
  })

  /*
   *  UDF for cust_est_income_range_desc
   *  Use age column from acxiom table to decide income bucket for customer
   */
  val getCustEstIncomeRangeDescUkAu = udf((incomeBucket: String) => {

    incomeBucket match {
      case "1" | "2" => IncomeBucket15K
      case "3" | "4" => IncomeBucket15K_30K
      case "5" | "6" => IncomeBucket30K_50K
      case "7" | "8" => IncomeBucket50K_75K
      case "9" => IncomeBucket75K_100K
      case "10" => IncomeBucket100KPlus
      case _ => UnknownBucket
    }
  })

  /*
   * UDFs for US demographic dimensions
   */

  /*
   *  UDF for cust_gender_code
   *  Use gender column from dim cust table to decide gender for customer
   */
  val getCustGenderCodeUs = udf((cnsldtd_gndr_cd: String, countryCode: String) => {

    countryCode match {

      case "US" => {
        cnsldtd_gndr_cd match {
          case GenderCodeMale => GenderCodeMale
          case GenderCodeFemale => GenderCodeFemale
          case _ => GenderCodeUnknown
        }
      }
      case _ => GenderCodeUnknown
    }
  })

  /*
   *  UDF for cust_gender_desc
   *  Use gender column from dim cust table to decide gender for customer
   */
  val getCustGenderDescriptionUs = udf((cnsldtd_gndr_cd: String, countryCode: String) => {

    countryCode match {

      case "US" =>
        cnsldtd_gndr_cd match {
          case GenderCodeMale => GenderDescMale
          case GenderCodeFemale => GenderDescFemale
          case _ => GenderDescUnknown
        }

      case _ => GenderDescUnknown
    }
  })

  /*
   *  UDF for cust_age_grp_code
   *  Use age column from dim cust table to decide age group for customer
   */
  val getCustAgeGrpCodeUS = udf((ageBucket: Int, countryCode: String) => {

    if (countryCode == "US") {
      if ((ageBucket >= 18) && (ageBucket <= 25)) AgeBucketCode1
      else if ((ageBucket >= 26) && (ageBucket <= 35)) AgeBucketCode2
      else if ((ageBucket >= 36) && (ageBucket <= 50)) AgeBucketCode3
      else if ((ageBucket >= 51) && (ageBucket <= 60)) AgeBucketCode4
      else if (ageBucket > 60) AgeBucketCode5
      else AgeBucketCodeUnknown
    }
    else AgeBucketCodeUnknown
  })

  /*
   *  UDF for cust_age_grp_desc
   *  Use age column from dim cust table to decide age group for customer
   */
  val getCustAgeGrpDescUs = udf((ageBucket: Int, countryCode: String) => {
    if (countryCode == "US") {
      if ((ageBucket >= 18) && (ageBucket <= 25)) AgeBucket18_25
      else if ((ageBucket >= 26) && (ageBucket <= 35)) AgeBucket26_35
      else if ((ageBucket >= 36) && (ageBucket <= 50)) AgeBucket36_50
      else if ((ageBucket >= 51) && (ageBucket <= 60)) AgeBucket51_60
      else if (ageBucket > 60) AgeBucket60Plus
      else UnknownBucket
    }
    else UnknownBucket
  })

  /*
   *  UDF for cust_est_income_range_code
   *  Use age column from dim cust table to decide income bucket for customer
   */
  val getCustEstIncomeRangeCodeUs = udf((incomeBucket: String, countryCode: String) => {

    countryCode match {
      case "US" =>
        incomeBucket match {
          case "1" => IncomeRangeCodeOne
          case "2" | "3" => IncomeRangeCodeTwo
          case "4" | "5" => IncomeRangeCodeThree
          case "6" => IncomeRangeCodeFour
          case "7" => IncomeRangeCodeFive
          case "8" | "9" => IncomeRangeCodeSix
          case _ => IncomeRangeCodeUnknown
        }
      case _ => IncomeRangeCodeUnknown
    }
  })

  /*
   *  UDF for cust_est_income_range_desc
   *  Use age column from acxiom table to decide income bucket for customer
   */
  val getCustEstIncomeRangeDescUs = udf((incomeBucket: String, countryCode: String) => {

    countryCode match {
      case "US" =>
        incomeBucket match {
          case "1" => IncomeBucket15K
          case "2" | "3" => IncomeBucket15K_30K
          case "4" | "5" => IncomeBucket30K_50K
          case "6" => IncomeBucket50K_75K
          case "7" => IncomeBucket75K_100K
          case "8" | "9" => IncomeBucket100KPlus
          case _ => UnknownBucket
        }
      case _ => UnknownBucket
    }
  })

  /*
   *  UDF for prmry_addr_state
   *  Use prmry_residence_code column from dim cust table to decide state for customer
   */
  val getPrimaryAddressState = udf((state: String) => {

    state match {
      case null => StateNotAvailable
      case _ =>
        val trimState = state.trim
        trimState match {
          case "#" | "--" | "X" | "XX" | "ZZ" | "II" | "N." | "N/A" | "na" | "VI" => StateNotAvailable
          case _ =>
            val trimStateNoNumber = trimState.replaceAll("[0-9]", "")
            trimStateNoNumber match {
              case "" => StateNotAvailable
              case _ => state
            }
        }
    }
  })

}
