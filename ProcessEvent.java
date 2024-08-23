package Flink;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessEvent implements MapFunction<String, String> {
    private static final Logger log = LoggerFactory.getLogger(ProcessEvent.class);
    private static final Gson gson = new Gson();

    @Override
    public String map(String value) throws Exception {
        String installSource = "";
        Integer currentExp = 0;
        String eventName = "";
        String aifa = "";
        Integer totalExp = 0;
        Double bannerRev = 0.0;
        Double interRev = 0.0;
        Double rewardRev = 0.0;
        Double openAdRev = 0.0;
        Double mrecRev = 0.0;
        Double totalRev = 0.0;

//        System.out.printf("%s\n",value);
        try {
            JSONObject jsonObject = new JSONObject(value);

            // Danh sách các trường và giá trị mặc định tương ứng
            String[] fields = {
                    "a", "e", "i", "k", "n", "aifa", "p", "s", "__TIMESTAMP__", "t", "u", "av",
                    "singular_install_id", "__TYPE__", "sdk", "seq"
            };

            // Lặp qua các trường và kiểm tra nếu chúng không tồn tại, thì thêm với giá trị mặc định
            for (String field : fields) {
                if (!jsonObject.has(field)) {
                    jsonObject.put(field, "");
                }
            }
            value = jsonObject.toString();

            // Parse JSON input into Event object
            Event event = gson.fromJson(value, Event.class);

            // Access basic fields
            eventName = event.getN() != null ? event.getN().trim() : ""; // Event Name
            aifa = event.getAifa() != null ? event.getAifa().trim() : ""; // AIFA
            Arguments e = event.getE() != null ? event.getE() : new Arguments();

            // Access user custom fields
            if (e.getUserData() != null) {
                Integer lineX1 = e.getUserData().getLineX1();
                Integer lineX2 = e.getUserData().getLineX2();
                Integer lineX3 = e.getUserData().getLineX3();
                Integer lineX4 = e.getUserData().getLineX4();
                Integer lineX5 = e.getUserData().getLineX5();
                Integer lineX6 = e.getUserData().getLineX6();
                Integer singlExp = e.getUserData().getSingleExp();
                Integer multiExp = e.getUserData().getMultiExp();
                Integer luckyExp = e.getUserData().getLuckyExp();
                totalExp = e.getUserData().getTotalExp() != null ? e.getUserData().getTotalExp() : 0;
                installSource = e.getUserData().getInstallSource() != null ? e.getUserData().getInstallSource() : "";
                bannerRev = e.getUserData().getBannerRev() != null ? e.getUserData().getBannerRev() : 0.0;
                interRev = e.getUserData().getInterRev() != null ? e.getUserData().getInterRev() : 0.0;
                rewardRev = e.getUserData().getRewardRev() != null ? e.getUserData().getRewardRev() : 0.0;
                openAdRev = e.getUserData().getOpenAdRev() != null ? e.getUserData().getOpenAdRev() : 0.0;
                mrecRev = e.getUserData().getMrecRev() != null ? e.getUserData().getMrecRev() : 0.0;

                currentExp = (lineX1 != null ? lineX1 : 0) * 100 +
                        (lineX2 != null ? lineX2 : 0) * 300 +
                        (lineX3 != null ? lineX3 : 0) * 600 +
                        (lineX4 != null ? lineX4 : 0) * 1000 +
                        (lineX5 != null ? lineX5 : 0) * 5 * 300 +
                        (lineX6 != null ? lineX6 : 0) * 6 * 300 +
                        (singlExp != null ? singlExp : 0) +
                        (multiExp != null ? multiExp : 0) +
                        (luckyExp != null ? luckyExp : 0);

                totalRev = bannerRev + interRev + rewardRev + openAdRev + mrecRev;
            }

            // Check conditions
            if (!"com.android.vending".equals(installSource)) {
                String updatedEventName = "CHEAT_SOURCE_" + eventName;
                JSONObject invalid = new JSONObject(value);
                invalid.put("n", updatedEventName);
                value = invalid.toString();
                Integer statusCode = Singular.Send(value);
                String notification = String.format("----- INVALID: AIFA: %s ----- EVENT: %s ----- CONVERTED TO: %s%n",
                        aifa, eventName, updatedEventName);
                System.out.printf(notification);
                log.info(notification);
                Telegram.check(statusCode, notification);
                return notification; // Return value or handle invalid events
            } else {
                if (totalRev > 0) {
                    if (currentExp.equals(totalExp)) {
                        Integer statusCode = Singular.Send(value);
                        String notification = String.format("----- VALID  : AIFA: %s ----- EVENT: %s ----- NO CONVERTED:%n",
                                aifa, eventName);
                        System.out.printf(notification);
                        log.info(notification);
                        Telegram.check(statusCode, notification);
                        return notification; // Return value as is or process as needed
                    } else {
                        String updatedEventName = "CHEAT_EXP_" + eventName;
                        JSONObject invalid = new JSONObject(value);
                        invalid.put("n", updatedEventName);
                        value = invalid.toString();
                        Integer statusCode = Singular.Send(value);
                        String notification = String.format("----- INVALID: AIFA: %s ----- EVENT: %s ----- CONVERTED TO: %s%n",
                                aifa, eventName, updatedEventName);
                        System.out.printf(notification);
                        log.info(notification);
                        Telegram.check(statusCode, notification);
                        return notification; // Return value or handle invalid events
                    }
                } else {
                    String updatedEventName = "CHEAT_AD_" + eventName;
                    JSONObject invalid = new JSONObject(value);
                    invalid.put("n", updatedEventName);
                    value = invalid.toString();
                    Integer statusCode = Singular.Send(value);
                    String notification = String.format("----- INVALID: AIFA: %s ----- EVENT: %s ----- CONVERTED TO: %s%n",
                            aifa, eventName, updatedEventName);
                    System.out.printf(notification);
                    log.info(notification);
                    Telegram.check(statusCode, notification);
                    return notification; // Return value or handle invalid events
                }
            }
        } catch (Exception e) {
            log.error("Error processing value {} --- error: {} \n", value, e.getMessage());
            //log.error("Error processing value: {}", e.getMessage());
            throw e;
        }
    }
}
