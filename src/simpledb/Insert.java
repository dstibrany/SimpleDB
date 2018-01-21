package simpledb;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {
    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private TupleDesc td;
    private boolean hasBeenCalled;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        this.tid = t;
        this.child = child;
        this.tableid = tableid;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});

        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid))) {
            throw new DbException("Child tuple descriptor does not match that of the table");
        }
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        this.hasBeenCalled = false;
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
        if (this.hasBeenCalled) return null;

        int insertCount = 0;
        hasBeenCalled = true;

        while (this.child.hasNext()) {
            Tuple tuple = this.child.next();
            insertCount++;
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableid, tuple);
            } catch (IOException e) {
                throw new DbException("Insert failed");
            }
        }

        Tuple insertResults = new Tuple(this.td);
        insertResults.setField(0, new IntField(insertCount));
        return insertResults;
    }
}
