package example.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TopkTest {  
  public static void main(String[] args) {
    int k = 3;  
    if (args.length > 0) {
        k = Integer.parseInt(args[0]);
    }
    
    List<Long> rawData = new ArrayList<Long>();
    List<Pair<String, Long>> array = new ArrayList<Pair<String,Long>>();
    MinHeap heap = new MinHeap(array);
  
    for(int i=0; i<10; ++i) {
      String a = "a" + i;
      Pair<String, Long> p1 = new Pair(a, (long)i);
      rawData.add(p1.getRight());
      heap.getTopK(p1,k);
    }
    for(int i=20; i>10; --i) {
      String a = "a" + i;
      Pair<String, Long> p1 = new Pair(a, (long)i);
      rawData.add(p1.getRight());
      heap.getTopK(p1,k);
    }
    Pair<String, Long> p2 = new Pair("p2", (long)12);
    rawData.add(p2.getRight());
    heap.getTopK(p2, k);
    
    Pair<String, Long> p3 = new Pair("p3", (long)5);
    rawData.add(p3.getRight());
    heap.getTopK(p3,k);
    
    Pair<String, Long> p = new Pair("p", (long)789);
    rawData.add(p.getRight());
    heap.getTopK(p,k);
     
    for(Pair<String, Long> t: heap.get()) {
      System.out.print(t.getLeft() + ":" + t.getRight() + " ");
    }
    System.out.println();
    
    heap.sort();
    for(Pair<String, Long> t: heap.get()) {
      System.out.print(t.getLeft() + ":" + t.getRight() + " ");
    }
    System.out.println();
    
    // verify result 
    Long[] arr = rawData.toArray(new Long[rawData.size()]);
    
    System.out.println("rawData:"+rawData);
    
    Arrays.sort(arr);
    System.out.println("sorted rawData: " + Arrays.toString(arr));
}
  
}
