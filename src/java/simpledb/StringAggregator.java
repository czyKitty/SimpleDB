package simpledb;
import java.util.*;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;

    private HashMap<Field,Integer> countMap;
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
        this.countMap = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field t_gbfield;
    	if (gbfield == Aggregator.NO_GROUPING) t_gbfield = null;
    	else t_gbfield = tup.getField(gbfield);
    	
    	if (countMap.containsKey(t_gbfield) == false) countMap.put(t_gbfield, 0);

    	int prevC = countMap.get(t_gbfield);
    	countMap.put(t_gbfield, prevC+1);
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
        TupleDesc td;
        
        // if not grouping, single type tuple
        if (gbfield == Aggregator.NO_GROUPING) {
            Type[] tps = new Type[] {Type.INT_TYPE};
            td = new TupleDesc(tps);
        }else {
        	Type[] tps = new Type[] {gbfieldtype, Type.INT_TYPE};
            td = new TupleDesc(tps);
        }
        
        Tuple newT;
        for (Field group : countMap.keySet()){
    		int aggVal = countMap.get(group);
    		newT = new Tuple(td);  
    		if (gbfield == Aggregator.NO_GROUPING) newT.setField(0, new IntField(aggVal));
    		else {
    			newT.setField(0, group);
        		newT.setField(1, new IntField(aggVal));    			
    		}
    		tList.add(newT);
    	}
    	return new TupleIterator(td, tList);
    }
}
