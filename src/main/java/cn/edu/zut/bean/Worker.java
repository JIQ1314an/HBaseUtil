package cn.edu.zut.bean;

/**
 * @Author jiquan
 * @Date 2021/6/3
 * @TIME 20:34
 */

public class Worker {
    private String id;
    private String name;
    private int gender; // 0或1表示
    private int age;
    private String phone;
    private String qq;
    private String weChat;

    public Worker() {
    }

    public Worker(String id) {
        this.id = id;
    }

    public Worker(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public Worker(String id, String name, int gender) {
        this.id = id;
        this.name = name;
        this.gender = gender;
    }

    public Worker(String id, String name, int gender, int age) {
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.age = age;
    }

    public Worker(String id, String name, int gender, int age, String phone) {

        this.id = id;
        this.name = name;
        this.gender = gender;
        this.age = age;
        this.phone = phone;
    }

    public Worker(String id, String name, int gender, int age, String phone, String qq) {
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.age = age;
        this.phone = phone;
        this.qq = qq;
    }

    public Worker(String id, String name, int gender, int age, String phone, String qq, String weChat) {
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.age = age;
        this.phone = phone;
        this.qq = qq;
        this.weChat = weChat;
    }


    @Override
    public String toString() {
        return "Worker{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", gender=" + gender +
                ", age=" + age +
                ", phone='" + phone + '\'' +
                ", qq='" + qq + '\'' +
                ", weChat='" + weChat + '\'' +
                '}';
    }
}
