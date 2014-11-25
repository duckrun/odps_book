package example.util;

import java.util.List;

/**
 * MinHeap: parent node is the minimal
 */
public class MinHeap
{
    private List<Pair<String, Long>> array_;
    private int heapSize_;
    
    // constructor
    public MinHeap(List<Pair<String, Long>> array){
      this.array_ = array;
      this.heapSize_ = 0;
    }
    
    // get topk results
    public void getTopK(Pair<String, Long> p, int k) {
      if(this.array_.size() < k) {
        this.array_.add(p);      
        return;
      }
      if(!this.isFull()){
        this.build();
      } 
      
      if (p.getRight() > this.min()) {
        this.replace(p);
        this.heapify(0);
      }   
    }
    
    public List<Pair<String, Long>> get() {
      return array_;
    }
    public int size(){
      return this.array_.size();
    }
    public int heapSize(){
      return this.heapSize_;
    }
    public boolean isFull(){
      return this.array_.size() == this.heapSize_;
    }
    
    public int compare(Pair<String, Long> a, Pair<String, Long> b) {
      if(a.getRight() < b.getRight()) 
        return -1;
      if(a.getRight() > b.getRight())
        return 1;
      return 0;
    } 
    
   //adjust to maintain mean heap structure
    public void heapify(int index) {
      int heapSize = this.heapSize_;
      if (index >= heapSize) {
        return;
      }
      int left = leftChildIndex(index);
      int right = rightChildIndex(index);
      int least = index;
      if (left < heapSize &&
              compare(array_.get(left), array_.get(index))  < 0) {
        least = left;
      }
      if (right < heapSize &&
          compare(array_.get(right), array_.get(least))  < 0) {
        least = right;
      }
      if (least != index) {
        exchange(index, least);
        heapify(least);
      }
    }    
    
    public void build() {
      for (int i = this.array_.size() / 2; i >= 0; --i) {
        heapify(i);
      }
      this.heapSize_ = this.array_.size();
    }
   
    public void sort() {
      build();
      for (int i = array_.size() -1; i >= 1; --i) {
        exchange(0, i);
        --heapSize_;
        heapify(0);
      }
    }
    
    public long min() {
        return array_.get(0).getRight();
    }
   
    public void replace(Pair<String, Long> a) {
        array_.set(0, a);
    }
    
    void exchange(int i, int j) {
        if (i >= array_.size() ||
                j >= array_.size()) {
            return;
        }        
        Pair<String, Long> tmp = array_.get(i);
        array_.set(i, array_.get(j));
        array_.set(j, tmp);
    }
    
    static int leftChildIndex(int i) {
        return i * 2 + 1;
    }
   
    static int rightChildIndex(int i) {
        return (i + 1) * 2;
    }    
}