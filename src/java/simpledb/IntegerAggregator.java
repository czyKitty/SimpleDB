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

    private HashMap<Field,Integer> aggMap;
    private HashMap<Field,Integer> countMap;
    
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
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.aggMap = new HashMap<Field, Integer>(); // value associate to field
        this.countMap = new HashMap<Field, Integer>(); // count associate to field
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    // base aggregate value
    private int baseAgg(){
    	switch(what){
	    	case MIN: return Integer.MAX_VALUE;
	    	case MAX: return Integer.MIN_VALUE;
	    	case SUM: 
	    	case COUNT: 
	    	case AVG: 
	    		return 0;
	    	default: 
	    		return 0;
    	}
    }
    
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field t_gbfield;
    	if (this.gbfield == NO_GROUPING) t_gbfield = null;
    	else t_gbfield = tup.getField(gbfield);
    	
    	if (aggMap.containsKey(t_gbfield) == false)
    	{
    		aggMap.put(t_gbfield, baseAgg());
    		countMap.put(t_gbfield, 0);
    	}
    	
        int val = ((IntField)tup.getField(afield)).getValue();
        int prevV = aggMap.get(t_gbfield);
        int prevC = countMap.get(t_gbfield);
        int newVal = prevV;
        
    	switch(what){
	    	case MIN: 
	    		if (val < prevV) newVal = val;
                break;
	    	case MAX: 
	    		if (val > prevV) newVal = val;
                break;
	    	case SUM: 
	    	case AVG: 
	    		countMap.put(t_gbfield, prevC+1);
	    		newVal = val+prevV;
                break;
	    	case COUNT: 
	    		newVal = prevV + 1;
	    	default: 
	    		break;
    	}
    	aggMap.put(t_gbfield, newVal);
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
    	ArrayList<Tuple> tList = new ArrayList<Tuple>();
        TupleDesc td = null;
        Tuple newT;
        
        // When not grouped, return tuple of single (aggregateVal)
        // Else, return tuple of pair
        if (gbfield == Aggregator.NO_GROUPING) {
            Type[] tps = new Type[1];
            tps[0] = Type.INT_TYPE;
            td = new TupleDesc(tps);
        }else {
        	Type[] tps = new Type[2];
            tps[0] = gbfieldtype;
            tps[1] = Type.INT_TYPE;
            td = new TupleDesc(tps);
        }
        	
        for (Field group : aggMap.keySet()){
    		int aggVal;
    		if (what == Op.AVG){
    			aggVal = aggMap.get(group) / countMap.get(group);
    		}
    		else
    		{
    			aggVal = aggMap.get(group);
    		}
    		newT = new Tuple(td);
    		if (gbfield == Aggregator.NO_GROUPING){
    			newT.setField(0, new IntField(aggVal));
    		}
    		else {
        		newT.setField(0, group);
        		newT.setField(1, new IntField(aggVal));    			
    		}
    		tList.add(newT);
    	}
    	return new TupleIterator(td, tList);
    }

}
