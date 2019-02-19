package simpledb;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;
import java.util.ArrayList;

/**
 * We reserve more heavy-duty deletion testing for HeapFile and HeapPage.
 * This suite is superficial.
 */
public class DeleteTest extends SimpleDbTestBase {

  private DbIterator tupleIterator;
  private HeapFile randomHeapFile;
  private TransactionId tid;

  private final int numRows = 400;

  /**
   * Initialize each unit test
   */
  @Before public void setUp() throws Exception {
    this.randomHeapFile = SystemTestUtil.createRandomHeapFile(2, this.numRows, null, null);
    TransactionId setupTid = new TransactionId();
    DbFileIterator setupIterator = randomHeapFile.iterator(setupTid);
    setupIterator.open();
    ArrayList<Tuple> tuples = new ArrayList<>();
    while (setupIterator.hasNext()) {
      tuples.add(setupIterator.next());
    }
    Database.getLockManager().removeTransaction(setupTid.hashCode());

    this.tid = new TransactionId();
    this.tupleIterator = new TupleIterator(new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE}), tuples);
  }

  /**
   * Unit test for Delete.getTupleDesc()
   */
  @Test public void getTupleDesc() throws Exception {
    Delete op = new Delete(tid, this.tupleIterator);
    TupleDesc expected = Utility.getTupleDesc(1);
    TupleDesc actual = op.getTupleDesc();
    assertEquals(expected, actual);
  }

  /**
   * Unit test for Delete.getNext(), deleting elements from a random heap file
   */
  @Test public void getNext() throws Exception {
    Delete op = new Delete(tid, tupleIterator);
    op.open();
    Tuple deleteResults = op.next();
    assertEquals(this.numRows, ((IntField) deleteResults.getField(0)).getValue());
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(DeleteTest.class);
  }
}

