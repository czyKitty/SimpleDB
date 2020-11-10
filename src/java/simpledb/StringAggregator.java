package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;

    private ArrayList<Field> gbList;
    private ArrayList<Integer> cList;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.gbList = new ArrayList<Field>();
        this.cList = new ArrayList<Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	int index;
        if (this.gbfield == NO_GROUPING) index = 0;
        else index = this.gbList.indexOf(tup.getField(this.gbfield));
        if (index == -1) index = this.gbList.size();
        
        if (index == this.cList.size()) {
            if (this.gbfield != NO_GROUPING) {
            	this.gbList.add(tup.getField(this.gbfield));
            }
            this.cList.add(1);
            return;
        }
        // Current count
        int prevC = this.cList.get(index);
        
        switch (what) {
            case COUNT:
            	this.cList.set(index, prevC + 1);
                break;
            default:
                break;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	ArrayList<Tuple> tList = new ArrayList<Tuple>();
    	
        TupleDesc td = null;
        // if not grouping, single type tuple
        if (this.gbfield == NO_GROUPING) {
            Type[] tps = new Type[1];
            tps[0] = Type.INT_TYPE;
            td = new TupleDesc(tps);
            Tuple t = new Tuple(td);
            
            int val = 0;
            
            switch (this.what) {
                case COUNT:
                    val = this.cList.get(0);
                    break;
                default:
                    break;
            }
            t.setField(0, new IntField(val));
            tList.add(t);
        } else {
        	// tuple with type pair
            Type[] tps = new Type[2];
            tps[0] = gbfieldtype;
            tps[1] = Type.INT_TYPE;
            td = new TupleDesc(tps);
            // Add tuple for each group
            for (int i = 0; i < this.cList.size(); i ++) {
                Tuple t = new Tuple(td);
                int val = 0;
                switch (what) {
                    case COUNT:
                        val = this.cList.get(i);
                        break;
                    default:
                        break;
                }
                t.setField(0, this.gbList.get(i));
                t.setField(1, new IntField(val));
                tList.add(t);
            }
        }
        return new TupleIterator(td, tList);
    }

}
