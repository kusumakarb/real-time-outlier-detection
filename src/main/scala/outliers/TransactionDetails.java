package outliers;

import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Abhishek on 6/9/17.
 */
public class TransactionDetails {
  private static final Logger logger = LoggerFactory.getLogger(TransactionDetails.class);

  private String unique_transact_id;
  private String card_num;
  private String processing_flag;
  private String trans_amt;
  private String trans_time;
  private String card_type;
  private String merchant_key;
  private String mcc_desc;
  private String category_name;
  private String city;
  private String zip;
  private String chrgbck_amt;
  private String chargeback_cat;
  private String chargeback_res;

  private static int transactionIdLength;
  private static int cardNumberLength;
  private static int merchantKeyLength;
  private static int mccDescLength;
  private static double baseTransactionAmount;
  private static double limitTransactionAmount;
  private static double baseChargebackAmount;
  private static double limitChargebackAmount;
  private static List<String> processingFlagList = new ArrayList<>();
  private static List<String> cardTypeList = new ArrayList<>();
  private static List<String> categoryNameList = new ArrayList<>();
  private static List<String> cityList = new ArrayList<>();
  private static List<String> chargebackCategoryList = new ArrayList<>();
  private static List<String> chargebackResolutionList = new ArrayList<>();

  private static Map<String, List<String>> cityZipcodeMap = new TreeMap<>();

  static {
    InputStream input = null;
    try {
      Properties prop = new Properties();
      input = TransactionDetails.class.getResourceAsStream("/transaction.properties");
      prop.load(input);

      transactionIdLength = Integer.parseInt(prop.getProperty("transactionIdLength"));
      cardNumberLength = Integer.parseInt(prop.getProperty("cardNumberLength"));
      merchantKeyLength = Integer.parseInt(prop.getProperty("merchantKeyLength"));
      mccDescLength = Integer.parseInt(prop.getProperty("mccDescLength"));

      baseTransactionAmount = Double.parseDouble(prop.getProperty("baseTransactionAmount"));
      limitTransactionAmount = Double.parseDouble(prop.getProperty("limitTransactionAmount"));
      baseChargebackAmount = Double.parseDouble(prop.getProperty("baseChargebackAmount"));
      limitChargebackAmount = Double.parseDouble(prop.getProperty("limitChargebackAmount"));

      processingFlagList = getPopulatedList(prop.getProperty("processingFlagList"));
      cardTypeList = getPopulatedList(prop.getProperty("cardTypeList"));
      categoryNameList = getPopulatedList(prop.getProperty("categoryNameList"));
      cityList = getPopulatedList(prop.getProperty("cityList"));
      chargebackCategoryList = getPopulatedList(prop.getProperty("chargebackCategoryList"));
      chargebackResolutionList = getPopulatedList(prop.getProperty("chargebackResolutionList"));

      for (String city:cityList)
        cityZipcodeMap.put(city, getPopulatedList(prop.getProperty("zipcode." + city)));

    } catch (IOException e) {
      logger.error(e.getMessage());
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public TransactionDetails(){
    unique_transact_id = getUnique_transact_id();
    card_num = getCard_num();
    processing_flag = getProcessing_flag();
    trans_amt = getTrans_amt();
    trans_time = getTrans_time();
    card_type = getCard_type();
    merchant_key = getMerchant_key();
    mcc_desc = getMcc_desc();
    category_name = getCategory_name();
    city = getCity();
    zip = getZip();
    chrgbck_amt = getChrgbck_amt();
    chargeback_cat = getChargeback_cat();
    chargeback_res = getChargeback_res();
  }

  private static List<String> getPopulatedList(String listValue) {
    return Arrays.asList(listValue.split(","));
  }

  public String getUnique_transact_id() {
    return RandomStringUtils.randomNumeric(transactionIdLength);
  }

  public String getCard_num() {
    return RandomStringUtils.randomNumeric(cardNumberLength);
  }

  public String getProcessing_flag() {
    return randomListElement(processingFlagList);
  }

  public String getTrans_amt() {
    return String.valueOf(ThreadLocalRandom.current().nextDouble(baseTransactionAmount, limitTransactionAmount));
  }

  public String getTrans_time() {
    return String.valueOf(System.currentTimeMillis());
  }

  public String getCard_type() {
    return randomListElement(cardTypeList);
  }

  public String getMerchant_key() {
    return RandomStringUtils.randomNumeric(merchantKeyLength);
  }

  public String getMcc_desc() {
    return RandomStringUtils.randomNumeric(mccDescLength);
  }

  public String getCategory_name() {
    return randomListElement(categoryNameList);
  }

  public String getCity() {
    return randomListElement(cityList);
  }

  public String getZip() {
    return randomListElement(cityZipcodeMap.get(getCity()));
  }

  public String getChrgbck_amt() {
    return String.valueOf(ThreadLocalRandom.current().nextDouble(baseChargebackAmount, limitChargebackAmount));
  }

  public String getChargeback_cat() {
    return randomListElement(chargebackCategoryList);
  }

  public String getChargeback_res() {
    return randomListElement(chargebackResolutionList);
  }

  private String randomListElement(List<String> randomList){
    return randomList.get(ThreadLocalRandom.current().nextInt(0, randomList.size()));
  }

  @Override
  public String toString() {
    return unique_transact_id + "," + card_num + "," + processing_flag + "," + trans_amt +
        "," + trans_time + "," + card_type + "," + merchant_key +
        "," + mcc_desc + "," + category_name + "," + city + "," + zip + "," + chrgbck_amt +
        "," + chargeback_cat + "," + chargeback_res;
  }
}
