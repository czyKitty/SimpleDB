package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child, agIter;
    private int afield, gfield;
    private Aggregator.Op aop;
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	// some code goes here
    	this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.agIter = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	// some code goes here
    	return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
    	if (this.gfield == Aggregator.NO_GROUPING) {
    		return null;
    	} else {
    		return child.getTupleDesc().getFieldName(this.gfield);
    	}
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
    	return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
    	return child.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
    	return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	if (this.child == null) throw new NoSuchElementException("Invalid children.");
        this.child.open();
        super.open();
        this.agIter = null;
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	// some code goes here
    	if (this.agIter == null) {
            if (this.child == null) return null;
            Aggregator agAgg = null;

            if (this.gfield == Aggregator.NO_GROUPING) {
            	return null;
            }else {
            	Type gfieldtype = child.getTupleDesc().getFieldType(gfield);
            	if (child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE))
                	agAgg = new IntegerAggregator(gfield, gfieldtype, afield, aop);
                else
                	agAgg = new StringAggregator(gfield, gfieldtype, afield, aop);
                while (child.hasNext())
                	agAgg.mergeTupleIntoGroup(child.next());
                this.agIter = agAgg.iterator();
                this.agIter.open();
            }
        }
    	if (this.agIter.hasNext()) return agIter.next();
        else return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	this.child.rewind();
        this.agIter = null;
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
    	Type[] tpArr = null;
        String[] fdArr = null;
        
        if (this.gfield == Aggregator.NO_GROUPING) {
        	tpArr = new Type[1];
        	tpArr[0] = Type.INT_TYPE;
        	fdArr = new String[1];
        	fdArr[0] = this.aop.toString() + "(" + this.child.getTupleDesc().getFieldName(this.afield) + ")";
        } else {
        	tpArr = new Type[2];
        	tpArr[0] = this.child.getTupleDesc().getFieldType(this.gfield);
        	tpArr[1] = Type.INT_TYPE;
            fdArr = new String[2];
            fdArr[0] = this.child.getTupleDesc().getFieldName(this.gfield);
            fdArr[1] = this.aop.toString() + "(" + this.child.getTupleDesc().getFieldName(this.afield) + ")";
        }
        return new TupleDesc(tpArr, fdArr);
    }

    public void close() {
	// some code goes here
    	super.close();
        this.child.close();
        this.agIter = null;
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
    	DbIterator[] children = new DbIterator[1];
        children[0] = this.child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
    	this.child = children[0];
    }
    
}
