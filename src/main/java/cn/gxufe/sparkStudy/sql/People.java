package cn.gxufe.sparkStudy.sql;

/**
 * @author 燕赤侠
 * @create 2016-08-28
 */
public class People implements java.io.Serializable{


    private static final long serialVersionUID = -422071446486457663L;


    private String name;
    private Integer age;


    public People(){}

    public People(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "People{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
