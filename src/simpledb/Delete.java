package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {
    private TransactionId tid;
    private DbIterator child;
    private TupleDesc td;
    private Boolean hasBeenCalled;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
       this.tid = t;
       this.child = child;
       this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
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
        this.close();
        this.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (this.hasBeenCalled) return null;

        int deleteCount = 0;
        this.hasBeenCalled = true;

        while (this.child.hasNext()) {
            Database.getBufferPool().deleteTuple(this.tid, this.child.next());
            deleteCount++;
        }

        Tuple deleteResults = new Tuple(this.td);
        deleteResults.setField(0, new IntField(deleteCount));
        return deleteResults;
    }
}
