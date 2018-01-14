package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {
    private int gbField;
    private Type gbFieldType;
    private int aggField;
    private Op aggOp;
    private HashMap<Field, Integer> groupCounts;
    private HashMap<Field, Integer> groupAggregates;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggField = afield;
        this.aggOp = what;
        this.groupCounts = new HashMap<>();
        this.groupAggregates = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field groupByField = getGroupByField(tup);

        int updatedCount = getUpdatedCount(tup);
        groupCounts.put(groupByField, updatedCount);

        int updatedAggregate = getUpdatedAggregate(tup, updatedCount);
        groupAggregates.put(groupByField, updatedAggregate);
    }

    /**
     * Returns the updated count aggregate (+1 for each tuple we've seen in a particular group)
     * @param tup The tuple used to update the current count
     * @return the updated count
     */
    private int getUpdatedCount(Tuple tup) {
        Field groupByField = getGroupByField(tup);
        int currentCount = groupCounts.getOrDefault(groupByField, 0);
        return ++currentCount;
    }

    /**
     * Returns the updated aggregate value, by adding the tuple aggregate value to the group aggregate value
     * @param tup The current tuple whose aggregate value we will merge into the group
     * @param currentCount The number of tuples we have seen in this group
     * @return the new aggregate value
     */
    private int getUpdatedAggregate(Tuple tup, int currentCount) {
        IntField tupleAggregate = (IntField)tup.getField((this.aggField));
        Field groupByField = getGroupByField(tup);
        Integer defaultValue = (this.aggOp == Op.MIN || this.aggOp == Op.MAX) ? tupleAggregate.getValue() : 0;
        Integer currentAggregateValue = groupAggregates.getOrDefault(groupByField, defaultValue);
        Integer updatedAggregateValue = currentAggregateValue;

        switch (this.aggOp) {
            case AVG:
                updatedAggregateValue = (currentAggregateValue + tupleAggregate.getValue()) / currentCount;
                break;
            case MAX:
                updatedAggregateValue = Math.max(currentAggregateValue, tupleAggregate.getValue());
                break;
            case MIN:
                updatedAggregateValue = Math.min(currentAggregateValue, tupleAggregate.getValue());
                break;
            case SUM:
                updatedAggregateValue = currentAggregateValue + tupleAggregate.getValue();
                break;
            case COUNT:
                updatedAggregateValue = currentCount;
                break;
        }
        return updatedAggregateValue;
    }

    /**
     * Returns the groupby Field of the current tuple, or null if there is no grouping
     * @param tup The tuple whose groupby Field we wish to return
     * @return the Field used in the group by clause, or null if there is no grouping
     */
    private Field getGroupByField(Tuple tup) {
        Field groupByField;
        if (this.gbField == Aggregator.NO_GROUPING) {
            groupByField = null;
        } else {
            groupByField = tup.getField(this.gbField);
        }
        return groupByField;
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
        TupleDesc groupAggregateTd;
        ArrayList<Tuple> tuples = new ArrayList<>();
        boolean hasGrouping = (this.gbField != Aggregator.NO_GROUPING);

        if (hasGrouping) {
            groupAggregateTd = new TupleDesc(new Type[]{this.gbFieldType, Type.INT_TYPE});
        } else {
            groupAggregateTd = new TupleDesc(new Type[]{Type.INT_TYPE});
        }

        for (HashMap.Entry<Field, Integer> groupAggregateEntry: this.groupAggregates.entrySet()) {
            Tuple groupAggregateTuple = new Tuple(groupAggregateTd);

            // If there is a grouping, we return a tuple in the form {groupByField, aggregateVal}
            // If there is no grouping, we return a tuple in the form {aggregateVal}
            if (hasGrouping) {
                groupAggregateTuple.setField(0, groupAggregateEntry.getKey());
                groupAggregateTuple.setField(1, new IntField(groupAggregateEntry.getValue()));
            } else {
                groupAggregateTuple.setField(0, new IntField(groupAggregateEntry.getValue()));
            }
            tuples.add(groupAggregateTuple);
        }
        return new TupleIterator(groupAggregateTd, tuples);
    }
}
