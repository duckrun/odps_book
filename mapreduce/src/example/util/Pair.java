package example.util;

import java.util.Comparator;

public class Pair<L,R> implements Comparator<Pair<L,R>> {

  private final L left;
  private final R right;
  
  public Pair(L left, R right) {
    this.left = left;
    this.right = right;
  }  
  
  public L getLeft() { return left; }
  public R getRight() { return right; }
  
  public int compare(Pair<L,R> a, Pair<L,R> b) {
    return ((Comparable)a.getRight()).compareTo((Comparable)b.getRight());    
  }
  
  @Override
  public int hashCode() { return left.hashCode() ^ right.hashCode(); }

  @Override
  public boolean equals(Object o) {
    if (o == null) return false;
    if (!(o instanceof Pair)) return false;
    Pair p = (Pair) o;
    return this.left.equals(p.getLeft()) &&
           this.right.equals(p.getRight());
  }
}
