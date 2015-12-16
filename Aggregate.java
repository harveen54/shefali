package simpledb;  

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
	
	private DbIterator dbIteratorLocal;//childIt
    private int aggregateColumnLocal;// aFieldIndex;
    private int groupingColumnLocal;// gFieldIndex;
    private Aggregator.Op operatorLocal;
    
    private Aggregator aggregatorLocal;//aggregator
    private DbIterator dbAggregateIteratorLocal;//aggregateIt;
    
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
    	dbIteratorLocal = child;
    	aggregateColumnLocal = afield;
    	groupingColumnLocal = gfield;
    	operatorLocal = aop;
    	dbAggregateIteratorLocal = null;

        Type type;
        
        type = (groupingColumnLocal == Aggregator.NO_GROUPING)? null :dbIteratorLocal.getTupleDesc().getType(groupingColumnLocal);
        
        Type aggregateType = dbIteratorLocal.getTupleDesc().getType(aggregateColumnLocal);
        if (aggregateType == Type.INT_TYPE){
        	aggregatorLocal = new IntAggregator(groupingColumnLocal,type,aggregateColumnLocal,operatorLocal);
        }
        if (aggregateType == Type.STRING_TYPE){
        	aggregatorLocal = new StringAggregator(groupingColumnLocal,type,aggregateColumnLocal,operatorLocal);
        }
    }

    
    
    
    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
    	
    	dbIteratorLocal.open();
        while(dbIteratorLocal.hasNext()){
            aggregatorLocal.merge(dbIteratorLocal.next());
        }
        dbAggregateIteratorLocal = aggregatorLocal.iterator();
        dbAggregateIteratorLocal.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (dbAggregateIteratorLocal.hasNext()){
            return dbAggregateIteratorLocal.next();
        }
      
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	dbAggregateIteratorLocal.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return dbIteratorLocal.getTupleDesc();
       
    }

    public void close() {
        // some code goes here
    	dbIteratorLocal.close();
    	dbAggregateIteratorLocal.close();
    }
}
