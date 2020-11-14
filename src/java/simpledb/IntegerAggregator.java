package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    private TupleDesc td;
    private HashMap<Field,ArrayList<Integer>> data; //[hold,count,sum]

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	this.data = new HashMap<Field, ArrayList<Integer>>();
    	if (gbfield==NO_GROUPING) td = new TupleDesc(new Type[]{Type.INT_TYPE});
    	else td = new TupleDesc(new Type[] {gbfieldtype,Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field f;
    	if (gbfield == NO_GROUPING) f = null;
    	f = tup.getField(gbfield);
    	int value = ((IntField)tup.getField(afield)).getValue();
    	if(data.containsKey(f)) {
    		int c,sum,temp;
    		temp=data.get(f).get(0);
    		c=data.get(f).get(1);
    		sum=data.get(f).get(2);
    		switch(what) {
	            case MIN:
	                if (value < temp) data.get(f).set(0, value);
	                break;
	            case MAX:
	                if (value > temp) data.get(f).set(0, value);
	                break;
	            case SUM:
	            case AVG:
	            	data.get(f).set(2, sum+value);
	            case COUNT:
	        		data.get(f).set(1, c+1);
	                break;
	            default:
	                break;
    		}
    	}
    	else {
    		data.put(f,new ArrayList<Integer>(Arrays.asList(value,1,value)));
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	Set<Field> keys = data.keySet();
    	Iterator<Field> keyItr = keys.iterator();
    	ArrayList<Tuple> tuplelist = new ArrayList<Tuple>();
    	while(keyItr.hasNext()){
    		Tuple t = new Tuple(td);
    		Field key = (Field)keyItr.next();
    		int value=0;
    		switch(what) {
	            case MIN:
	                value=data.get(key).get(0);
	                break;
	            case MAX:
	            	value=data.get(key).get(0);
	                break;
	            case SUM:
	            	value=data.get(key).get(2);
	            	break;
	            case AVG:
	            	value=data.get(key).get(2)/data.get(key).get(1);
	            	break;
	            case COUNT:
	            	value=data.get(key).get(1);
	                break;
    		}
    		if(gbfield == NO_GROUPING){
    			t.setField(0, new IntField(value));
    		}else{
    			t.setField(0, key);
    			t.setField(1, new IntField(value));
    		}
    		tuplelist.add(t);
    	}
    	return new TupleIterator(td, tuplelist);
    }

}
