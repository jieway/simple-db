package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;

    private Type gbFieldtype;

    private int aField;

    private Op what;

    private Map<Field , Integer> fieldCount;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldtype = gbfieldtype;
        this.aField = afield;
        this.what = what;
        fieldCount = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbField != Aggregator.NO_GROUPING) {
            Field field = tup.getField(this.gbField);
            if (fieldCount.containsKey(field)) {
                fieldCount.put(field , fieldCount.get(field) + 1);
            }else {
                fieldCount.put(field , 1);
            }
        }else {
            if (fieldCount.containsKey(null)) {
                fieldCount.put(null, fieldCount.get(null) + 1);
            } else {
                fieldCount.put(null, 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {

            Iterator<Map.Entry<Field, Integer>> iterator;

            TupleDesc td;

            Tuple tuple;


            @Override
            public void open() throws DbException, TransactionAbortedException {
                iterator = fieldCount.entrySet().iterator();
                if (gbField != NO_GROUPING) {
                    if (td == null) {
                        Type[] typeArr = new Type[2];
                        typeArr[0] = gbFieldtype;
                        typeArr[1] = Type.INT_TYPE;
                        td = new TupleDesc(typeArr);
                        tuple = new Tuple(td);
                    }
                } else {
                    if (td == null) {
                        Type[] typeArr = new Type[1];
                        typeArr[0] = Type.INT_TYPE;
                        td = new TupleDesc(typeArr);
                        tuple = new Tuple(td);
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (iterator == null) throw new DbException("not yet open");
                if (iterator.hasNext()) return true;
                else return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (iterator == null) throw new NoSuchElementException();
                Map.Entry<Field, Integer> next = iterator.next();
                if (gbField != NO_GROUPING) {
                    tuple.setField(0, next.getKey());
                    Field field = new IntField(next.getValue());
                    tuple.setField(1, field);
                } else {
                    Field field = new IntField(next.getValue());
                    tuple.setField(0, field);
                }
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                if (gbField != NO_GROUPING) {
                    if (td == null) {
                        Type[] typeArr = new Type[2];
                        typeArr[0] = gbFieldtype;
                        typeArr[1] = Type.INT_TYPE;
                        return new TupleDesc(typeArr);
                    }
                } else {
                    if (td == null) {
                        Type[] typeArr = new Type[1];
                        typeArr[0] = Type.INT_TYPE;
                        return new TupleDesc(typeArr);
                    }
                }
                return null;
            }

            @Override
            public void close() {
                iterator = null;
                td = null;
                tuple = null;
            }
        };
    }
}
