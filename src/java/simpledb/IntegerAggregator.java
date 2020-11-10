package simpledb;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;

    private ArrayList<Field> gbList;
    private ArrayList<Integer> vList, cList;
    
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
        this.gbList = new ArrayList<Field>();
        this.vList = new ArrayList<Integer>(); // List of value associate to field
        this.cList = new ArrayList<Integer>(); // List of count associate to field
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
    	int index;
    	if (this.gbfield == NO_GROUPING) {
    		index = 0;
    	}else {
    		index = this.gbList.indexOf(tup.getField(this.gbfield));
    	}
    	
        int val = ((IntField)tup.getField(afield)).getValue();
        
        if (index == vList.size()) {
            if (gbfield != NO_GROUPING){
            	gbList.add(tup.getField(this.gbfield));
            }
            vList.add(val);
            cList.add(1);
            return;
        }
    	// Record current value and count
        int prevV = vList.get(index);
        int prevC = cList.get(index);
        
    	switch(this.what){
	    	case MIN: 
	    		if (val <  prevV) vList.set(index, val);
                break;
	    	case MAX: 
	    		if (val > prevV) vList.set(index, val);
                break;
	    	case SUM: 
	    	case COUNT: 
	    	case AVG: 
	    		vList.set(index, val + prevV);
                cList.set(index, prevC + 1);
                break;
	    	default: 
	    		break;
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
    	ArrayList<Tuple> tList = new ArrayList<Tuple>();
        TupleDesc td = null;

        // When not grouped, return tuple of single (aggregateVal)
        // Else, return tuple of pair
        if (gbfield == NO_GROUPING) {
            Type[] tps = new Type[1];
            tps[0] = Type.INT_TYPE;
            td = new TupleDesc(tps);
            Tuple t = new Tuple(td);
            int val = 0;
            switch (what) {
                case MIN:
                case MAX:
                case SUM:
                    val = vList.get(0);
                    break;
                case COUNT:
                    val = cList.get(0);
                    break;
                case AVG:
                    val = vList.get(0)/cList.get(0);
                    break;
                default:
                    break;
            }
            t.setField(0, new IntField(val));
            tList.add(t);
        }else {
            Type[] tps = new Type[2];
            tps[0] = gbfieldtype;
            tps[1] = Type.INT_TYPE;
            td = new TupleDesc(tps);
            // Add tuple for each group
            for (int i = 0; i < vList.size(); i ++) {
                Tuple t = new Tuple(td);
                int val = 0;
                switch (what) {
                    case MIN:
                    case MAX:
                    case SUM:
                        val = vList.get(i);
                        break;
                    case COUNT:
                        val = cList.get(i);
                        break;
                    case AVG:
                        val = vList.get(i)/cList.get(i);
                        break;
                    default:
                        break;
                }
                t.setField(0, gbList.get(i));
                t.setField(1, new IntField(val));
                tList.add(t);
            }
        }
        return new TupleIterator(td, tList);
    }

}
