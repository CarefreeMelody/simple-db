package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbFieldIndex;
    private Type gbFieldType;
    private int aFieldIndex;
    private AggHandler aggHandler;
     
    private abstract class AggHandler {
        HashMap<Field, Integer> aggResult;

        abstract void handle(Field gbfield, StringField aggField);

        public AggHandler() {
            this.aggResult = new HashMap<>();
        }

        public HashMap<Field, Integer> getAggResult() {
            return this.aggResult;
        }
    }

    private class CountHandler extends AggHandler {
        @Override
        void handle(Field gbfield, StringField aggField) {
            aggResult.put(gbfield, aggResult.getOrDefault(gbfield, 0) + 1);
        }
    }

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFieldIndex = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aFieldIndex = afield;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }else {
            aggHandler = new CountHandler();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField = gbFieldIndex == NO_GROUPING ? null : tup.getField(gbFieldIndex);
        StringField aggField = (StringField) (tup.getField(aFieldIndex));
        aggHandler.handle(gbField, aggField);
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
        HashMap<Field, Integer> result = aggHandler.getAggResult();
        Type[] fieldTypes;
        String[] fieldNames;
        TupleDesc tupleDesc;
        List<Tuple> tuples = new ArrayList<>();
        if (gbFieldIndex == NO_GROUPING) {
            fieldTypes = new Type[]{Type.INT_TYPE};
            fieldNames = new String[]{"AggregateValue"};
            tupleDesc = new TupleDesc(fieldTypes, fieldNames);
            Tuple tuple = new Tuple(tupleDesc);
            IntField resultField = new IntField(result.get(null));
            tuple.setField(0, resultField);
            tuples.add(tuple);
        }else {
            fieldTypes = new Type[]{gbFieldType, Type.INT_TYPE};
            fieldNames = new String[]{"GroupByValue", "AggregateValue"};
            tupleDesc = new TupleDesc(fieldTypes, fieldNames);
            for (Field field : result.keySet()) {
                Tuple tuple = new Tuple(tupleDesc);
                if (gbFieldType == Type.INT_TYPE) {
                    IntField gbField = (IntField) field;
                    tuple.setField(0, gbField);
                }else {
                    StringField gbField = (StringField) field;
                    tuple.setField(0, gbField);
                }
                IntField resultField = new IntField(result.get(field));
                tuple.setField(1, resultField);
                tuples.add(tuple);
            }
        } 
        return new TupleIterator(tupleDesc, tuples);
    }

}
