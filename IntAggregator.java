package simpledb;

import java.util.Hashtable;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

	public int zeroBasedIndexGroupLocal;
    public Type groupTypeLocal;
    public int zeroBasedIndexAggLocal;
    public Op operatorLocal;

    public Hashtable<Field,Integer> localGroup;
    public Hashtable<Field, Integer> localCount;
	
    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	zeroBasedIndexGroupLocal = gbfield;
    	groupTypeLocal = gbfieldtype;
    	zeroBasedIndexAggLocal = afield;
    	operatorLocal = what;
    	localGroup = new Hashtable<Field, Integer>();
    	localCount = new Hashtable<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // some code goes here
    	
    	 Field groupFieldMergeLocal= (zeroBasedIndexGroupLocal == NO_GROUPING) ? null:tup.getField(zeroBasedIndexGroupLocal);
         
    	 if (localGroup.containsKey(groupFieldMergeLocal)==false){
    		 AddIteminLocalGroup(groupFieldMergeLocal);
    		 }
    	 
         int countOld = localCount.get(groupFieldMergeLocal);
         localCount.put(groupFieldMergeLocal,countOld++);
         
         AssignValueToOperator(operatorLocal,groupFieldMergeLocal,tup,countOld);
        
    }

    
    private void AssignValueToOperator(Op operatorLocal,Field groupFieldMergeLocal,Tuple tup,int countOld)
    {
    	IntField integerField= (IntField)tup.getField(zeroBasedIndexAggLocal);
        int valueOriginal = integerField.getValue();
        
        int valueOld = localGroup.get(groupFieldMergeLocal);
        int valueNew=valueOld;
    	 switch (operatorLocal){
         case MAX:
        	 valueNew = Math.max(valueOriginal,valueOld);
             break;
         case MIN:
        	 valueNew = Math.min(valueOriginal,valueOld);
             break;
         case AVG:
        	 valueNew = (valueOld + valueOriginal)/countOld;
             break;
         case SUM:
        	 valueNew = valueOld + valueOriginal;
             break;
         case COUNT:
        	 valueNew = valueOld+1;
             break;
         default:
             break;
     }
     localGroup.put(groupFieldMergeLocal,valueNew);
    }
    
    private void AddIteminLocalGroup(Field groupFieldMergeLocal)
    {
    	localGroup.put(groupFieldMergeLocal,0);
		 localCount.put(groupFieldMergeLocal, 0);
		  if (operatorLocal == Op.MAX){
       	 localGroup.put(groupFieldMergeLocal,Integer.MIN_VALUE);
        }
        if (operatorLocal == Op.MIN){
       	 localGroup.put(groupFieldMergeLocal,Integer.MAX_VALUE);
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
        TupleDesc tupleDesc = (zeroBasedIndexGroupLocal == Aggregator.NO_GROUPING)? (new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggregateValue"})):(new TupleDesc(new Type[] {groupTypeLocal,Type.INT_TYPE}, new String[] {"groupValue","aggregateValue"}));
        Tuple tupleNew;
        int valueAggregateLocal;
        
        	for(Field i : localGroup.keySet() )
        {
        		valueAggregateLocal=(operatorLocal == Op.AVG)?localGroup.get(i)/localCount.get(i):localGroup.get(i);

            tupleNew = new Tuple(tupleDesc);
            
            if (zeroBasedIndexGroupLocal == Aggregator.NO_GROUPING){
            	tupleNew.setField(0, new IntField(valueAggregateLocal));
            }
            else{
            	
            	tupleNew.setField(0,i);
            	tupleNew.setField(1, new IntField(valueAggregateLocal));
            }
            tupleList.add(tupleNew);
        }
        return new TupleIterator(tupleDesc,tupleList);
       }
    
    
    
    
}
