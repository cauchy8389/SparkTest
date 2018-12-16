package SparkSqlJava;

import java.io.Serializable;

/**
 * Created by Administrator on 2018/9/9.
 */

/*
Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 0.0 in stage 0.0 (TID 0) had a not serializable result: SparkSqlJava.Student
Serialization stack:
	- object not serializable (class: SparkSqlJava.Student, value: SparkSqlJava.Student@503cb89d)
	- element of array (index: 0)
	- array (class [Ljava.lang.Object;, size 2)
	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$failJobAndIndependentStages(DAGScheduler.scala:1457)

    对于spark  RDD中用到任何自定义的类，都需要序列化。因为会发生数据网络之间的传输
 */

public class Student implements Serializable{
    private int id = 0;
    private String name = "";
    private int age = 0;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }


}
