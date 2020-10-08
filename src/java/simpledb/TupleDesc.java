package simpledb;

import java.io.Serializable;
import java.util.*;

import simpledb.TupleDesc.TDItem;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
    	ArrayList<TDItem> TDItems = new ArrayList<TDItem>();
        return TDItems.iterator();
    }

    private static final long serialVersionUID = 1L;
    private ArrayList<TDItem> TD;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here	
    	this.TD = new ArrayList<TDItem>();
    	for(int i=0; i < typeAr.length; i++) {
    		this.TD.add(new TDItem(typeAr[i],fieldAr[i]));
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	this.TD = new ArrayList<TDItem>();
    	for(int i=0; i < typeAr.length; i++) {
    		this.TD.add(new TDItem(typeAr[i],null));
    	}
    	
    }
    
    public TupleDesc(ArrayList<TDItem> TD) {
    	this.TD = TD;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.TD.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	try {
    		return this.TD.get(i).fieldName;
    	}
    	catch (Exception e){
    		throw new NoSuchElementException();
    	}
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	try {
    		return this.TD.get(i).fieldType;
    	}
    	catch (Exception e){
    		throw new NoSuchElementException();
    	}
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	for (int i = 0; i < this.numFields();i++) {
    		if(this.getFieldName(i) != null && this.getFieldName(i).equals(name)){
    			return i;
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int size=0;
    	for(int i =0;i<this.numFields();i++) {
    		size+=this.TD.get(i).fieldType.getLen();
    		}
    	return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	ArrayList<TDItem> merge = new ArrayList<TDItem>();
    	merge.addAll(td1.TD);
    	merge.addAll(td2.TD);
        return new TupleDesc(merge);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
    	try {
    		TupleDesc obj = TupleDesc.class.cast(o);
    		if (obj.numFields()==this.numFields()) {
    			for(int i =0;i<this.numFields();i++) {
    				if(!obj.TD.get(i).fieldType.equals(this.TD.get(i).fieldType)) {
    					return false;
    				}
    			}
    			return true;
    		}
    	}
    	catch (Exception e){
    	}
    	return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	String desc = "";
    	for (int i= 0; i<this.numFields(); i++) {
    		desc += "fieldType["+this.TD.get(i).fieldType+"](fieldName["+this.TD.get(i).fieldName+"])\n";
    	}
        return desc;
    }
}
