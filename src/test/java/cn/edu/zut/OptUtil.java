package cn.edu.zut;

/**
 * @Author 86131
 * @Date 2021/6/3
 * @TIME 21:06
 */
public class OptUtil {

    public static String fill(Object data, int length) {
        if (data == null) return "";
        String str = data.toString();
        while (str.length() < length) {
            str = "0" + str;
        }
        return str;
    }

    public static int getRandom(int min, int max) {
        return (int) (Math.random() * (max - min + 1) + min);
    }

    public static String getPhone() {
        String phone = "1";
        while (phone.length() < 12) {
            phone += getRandom(1, 9);
        }

        return phone;
    }
}
