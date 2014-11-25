package example;
import example.IP2num;

public class Test {
  static IP2num t = new IP2num();
  public static void main(String[] args) {
    String s = "192.168.7.2";
    System.out.println(t.evaluate(s));
  }
}
