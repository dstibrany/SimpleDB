package simpledb;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFile file;
    private DbFileIterator fileIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.file = Database.getCatalog().getDbFile(tableid);
        this.fileIterator = this.file.iterator(tid);
    }

    public void open()
        throws DbException, TransactionAbortedException {
        this.fileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    // TODO implement this
    public TupleDesc getTupleDesc() {
        // TupleDesc originalTd = this.file.getTupleDesc();
        // TupleDesc newTd = new TupleDesc
        return null;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return this.fileIterator.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        return this.fileIterator.next();
    }

    public void close() {
        this.fileIterator.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        this.fileIterator.rewind();
    }
}
