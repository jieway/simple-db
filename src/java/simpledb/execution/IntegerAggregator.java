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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;

    private Type gbFieldType;

    private int aField;

    private Op what;

    private int count;

    private int sum;

    // field sum
    private HashMap<Field , Integer> fieldSum;

    // field count
    private HashMap<Field , Integer> fieldCount;

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
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;
        if (gbfield != Aggregator.NO_GROUPING) {
            fieldSum = new HashMap<>();
            fieldCount = new HashMap<>();
        }else {
            count = 0;
            sum = 0;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbField != Aggregator.NO_GROUPING) {
            Field field = tup.getField(gbField);
            if (fieldSum.containsKey(field)) {
                if (this.what == Op.AVG) {
                    fieldSum.put(field , fieldSum.get(field) +
                            ((IntField)tup.getField(aField)).getValue());
                    fieldCount.put(field , fieldCount.get(field) + 1);
                }else if (this.what == Op.COUNT) {
                    fieldSum.put(field, fieldSum.get(field) + 1);
                }else if (this.what == Op.SUM) {
                    fieldSum.put(field , fieldSum.get(field) +
                            ((IntField)tup.getField(aField)).getValue());
                }else if (this.what == Op.MAX) {
                    fieldSum.put(field , Math.max(fieldSum.get(field) ,
                                    ((IntField)tup.getField(aField)).getValue()));
                }else if (this.what == Op.MIN) {
                    fieldSum.put(field , Math.min(fieldSum.get(field) ,
                            ((IntField)tup.getField(aField)).getValue()));
                }
            } else {
                if (this.what == Op.AVG) {
                    fieldSum.put(field , ((IntField)tup.getField(aField)).getValue());
                    fieldCount.put(field , 1);
                }else if (this.what == Op.COUNT) {
                    fieldSum.put(field ,  1);
                }else if (this.what == Op.SUM) {
                    fieldSum.put(field , ((IntField)tup.getField(aField)).getValue());
                }else if (this.what == Op.MAX) {
                    fieldSum.put(field , ((IntField)tup.getField(aField)).getValue());
                }else if (this.what == Op.MIN) {
                    fieldSum.put(field , ((IntField)tup.getField(aField)).getValue());
                }
            }
        }else {
            if (this.what == Op.AVG) {
                count ++;
                sum += ((IntField)tup.getField(aField)).getValue();
            }else if (this.what == Op.COUNT) {
                count++;
                sum++;
            }else if (this.what == Op.SUM) {
                sum += ((IntField)tup.getField(aField)).getValue();
                count ++;
            }else if (this.what == Op.MAX) {
                int t = ((IntField)tup.getField(aField)).getValue();
                if (count == 0) sum = t;
                else sum = Math.max(sum , t);
            }else if (this.what == Op.MIN) {
                int t = ((IntField)tup.getField(aField)).getValue();
                if (count == 0) sum = t;
                else sum = Math.min(sum , t);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {

            Iterator<Map.Entry<Field, Integer>> iterator;

            TupleDesc td;

            Tuple tuple;

            private Boolean hasNext;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (gbField == Aggregator.NO_GROUPING) {
                    if (td == null) {
                        Type[] typeArr = new Type[1];
                        typeArr[0] = Type.INT_TYPE;
                        td = new TupleDesc(typeArr);
                        tuple = new Tuple(td);
                    }
                    hasNext = true;
                }else {
                    iterator = fieldSum.entrySet().iterator();
                    if (td == null) {
                        Type[] typeArr = new Type[2];
                        typeArr[0] = gbFieldType;
                        typeArr[1] = Type.INT_TYPE;
                        td = new TupleDesc(typeArr);
                        tuple = new Tuple(td);
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (gbField != NO_GROUPING) {
                    if (iterator == null) throw new DbException("not yet open");
                    if (iterator.hasNext()) return true;
                    else return false;
                } else {
                    if (td == null) throw new DbException("not yet open");
                    return hasNext;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (gbField != NO_GROUPING) {
                    if (iterator == null) throw new NoSuchElementException();
                    Map.Entry<Field, Integer> next = iterator.next();
                    tuple.setField(0, next.getKey());
                    if (what == Op.AVG) {
                        Field field = new IntField(next.getValue() / fieldCount.get(next.getKey()));
                        tuple.setField(1, field);
                    } else {
                        Field field = new IntField(next.getValue());
                        // System.out.println(next.getValue());
                        tuple.setField(1, field);
                    }
                } else {
                    if (td == null) throw new NoSuchElementException();
                    if (what == Op.AVG) {
                        Field field = new IntField(sum / count);
                        tuple.setField(0, field);
                    } else {
                        Field field = new IntField(sum);
                        tuple.setField(0, field);
                    }
                    hasNext = false;
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
                        typeArr[0] = gbFieldType;
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
