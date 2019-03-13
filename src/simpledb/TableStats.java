package simpledb;

import java.util.Arrays;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    private int[] minPerField;
    private int[] maxPerField;
    private int ioCostPerPage;
    private IntHistogram[] intHistograms;
    private StringHistogram[] stringHistograms;
    private int tableid;
    private TupleDesc tupleDesc;
    private int numTuples;

    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        this.tableid = tableid;
        tupleDesc = Database.getCatalog().getTupleDesc(tableid);
        intHistograms = new IntHistogram[tupleDesc.numFields()];
        stringHistograms = new StringHistogram[tupleDesc.numFields()];
        minPerField = new int[tupleDesc.numFields()];
        maxPerField = new int[tupleDesc.numFields()];
        Arrays.fill(minPerField, Integer.MAX_VALUE);
        Arrays.fill(maxPerField, Integer.MIN_VALUE);

        try {
            getMinMaxPerField();
            generateHistograms();
            countNumTuples();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }

    }

    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
        int numPages = ((HeapFile) Database.getCatalog().getDbFile(tableid)).numPages();
    	return ioCostPerPage * numPages;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	return (int) (numTuples * selectivityFactor);
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        double selectivity = 0;

        if (tupleDesc.getType(field) == Type.INT_TYPE) {
            selectivity = intHistograms[field].estimateSelectivity(op, ((IntField)constant).getValue());
        } else if (tupleDesc.getType(field) == Type.STRING_TYPE) {
            selectivity = stringHistograms[field].estimateSelectivity(op, ((StringField)constant).getValue());
        }
        return selectivity;
    }

    private void getMinMaxPerField() throws DbException, TransactionAbortedException {
        DbFileIterator iterator = Database.getCatalog().getDbFile(tableid).iterator(new TransactionId());

        iterator.open();

        while (iterator.hasNext()) {
            Tuple t = iterator.next();
            for (int fieldIdx = 0; fieldIdx < tupleDesc.numFields(); fieldIdx++) {
                if (tupleDesc.getType(fieldIdx) == Type.INT_TYPE) {
                    if (((IntField)t.getField(fieldIdx)).getValue() < minPerField[fieldIdx]) {
                        minPerField[fieldIdx] = ((IntField)t.getField(fieldIdx)).getValue();
                    }
                    if (((IntField)t.getField(fieldIdx)).getValue() > maxPerField[fieldIdx]) {
                        maxPerField[fieldIdx] = ((IntField)t.getField(fieldIdx)).getValue();
                    }
                }
            }
        }
        iterator.close();
    }

    private void generateHistograms() throws DbException, TransactionAbortedException {
        DbFileIterator iterator = Database.getCatalog().getDbFile(tableid).iterator(new TransactionId());

        iterator.open();

        while (iterator.hasNext()) {
            Tuple t = iterator.next();
            for (int fieldIdx = 0; fieldIdx < t.numFields(); fieldIdx++) {
                if (tupleDesc.getType(fieldIdx) == Type.INT_TYPE) {
                    intHistograms[fieldIdx] = (intHistograms[fieldIdx] == null) ?
                            new IntHistogram(NUM_HIST_BINS, minPerField[fieldIdx], maxPerField[fieldIdx]) :
                            intHistograms[fieldIdx];
                    IntField f = (IntField) t.getField(fieldIdx);
                    intHistograms[fieldIdx].addValue(f.getValue());
                } else if (tupleDesc.getType(fieldIdx) == Type.STRING_TYPE) {
                    stringHistograms[fieldIdx] = (stringHistograms[fieldIdx] == null) ?
                            new StringHistogram(NUM_HIST_BINS) :
                            stringHistograms[fieldIdx];
                    StringField f = (StringField) t.getField(fieldIdx);
                    stringHistograms[fieldIdx].addValue(f.getValue());
                }
            }
        }

        iterator.close();
    }

    private void countNumTuples() throws DbException, TransactionAbortedException {
        DbFileIterator iterator = Database.getCatalog().getDbFile(tableid).iterator(new TransactionId());

        iterator.open();

        while (iterator.hasNext()) {
            Tuple t = iterator.next();
            numTuples++;
        }
        iterator.close();
    }

}
