package simpledb;

import simpledb.Aggregator.Op;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield,afield;
    private Op what;
    private Type gbfieldtype;
    private TupleDesc td;
    private HashMap<Field,Integer> count;

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
    	this.gbfield=gbfield;
    	this.gbfieldtype=gbfieldtype;
    	this.afield=afield;
    	this.what=what;
    	this.count = new HashMap<Field, Integer>();
    	if (what!= Aggregator.Op.COUNT) throw new IllegalArgumentException();
    	if (gbfield==NO_GROUPING) td = new TupleDesc(new Type[]{Type.INT_TYPE});
    	else td = new TupleDesc(new Type[] {gbfieldtype,Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field f;
    	if (gbfield == NO_GROUPING) f = null;
    	f = tup.getField(gbfield);
    	int c = 1;
    	if(count.containsKey(f)) {
	        c=count.get(f);
	        c++;
    	}
    	count.put(f, c);
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
    	Set<Field> keys = count.keySet();
    	Iterator<Field> keyItr = keys.iterator();
    	ArrayList<Tuple> tuplelist = new ArrayList<Tuple>();
    	while(keyItr.hasNext()){
    		Tuple t = new Tuple(td);
    		Field key = (Field)keyItr.next();
    		if(gbfield == NO_GROUPING){
    			t.setField(0, new IntField(count.get(key)));
    		}else{
    			t.setField(0, key);
    			t.setField(1, new IntField(count.get(key)));
    		}
    		tuplelist.add(t);
    	}
    	return new TupleIterator(td, tuplelist);
    }

}
