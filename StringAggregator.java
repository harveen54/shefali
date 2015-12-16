package simpledb;

import java.util.Hashtable;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
	
	public int zeroBasedIndexGroupLocal;
    public Type groupTypeLocal;
    public int zeroBasedIndexAggLocal;
    public Op operatorLocal;

    public Hashtable<Field,Integer> localGroup;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	zeroBasedIndexGroupLocal = gbfield;
    	groupTypeLocal = gbfieldtype;
    	zeroBasedIndexAggLocal = afield;
    	operatorLocal = what;
    	localGroup = new Hashtable<Field, Integer>();
    	if (operatorLocal!=Op.COUNT){
            throw new IllegalArgumentException("Since input is a string so only COUNT is supported");
        }
      }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // some code goes here
    	Field groupFieldMergeLocal= (zeroBasedIndexGroupLocal == NO_GROUPING) ? null:tup.getField(zeroBasedIndexGroupLocal);
    	
    	
        if (localGroup.containsKey(groupFieldMergeLocal)==false){
        	localGroup.put(groupFieldMergeLocal,1);
        }
        else {
        	localGroup.put(groupFieldMergeLocal,localGroup.get(groupFieldMergeLocal) + 1);
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
    	List<Tuple> tupleList = new ArrayList<Tuple>();
    	TupleDesc groupDesc = (zeroBasedIndexGroupLocal == Aggregator.NO_GROUPING)? (new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggregateValue"})):(new TupleDesc(new Type[] {groupTypeLocal,Type.INT_TYPE}, new String[] {"groupValue","aggregateValue"}));
    	Tuple tupleNew;
        int valueAggregateLocal;
        for(Field i : localGroup.keySet()){

        	valueAggregateLocal = localGroup.get(i);
        	tupleNew = new Tuple(groupDesc);
        	
        	if (zeroBasedIndexGroupLocal == Aggregator.NO_GROUPING){
            	tupleNew.setField(0, new IntField(valueAggregateLocal));
            }
            else{
            	
            	tupleNew.setField(0,i);
            	tupleNew.setField(1, new IntField(valueAggregateLocal));
            }
            tupleList.add(tupleNew);
       
        	
            
        }
        return new TupleIterator(groupDesc,tupleList);
        }

}
