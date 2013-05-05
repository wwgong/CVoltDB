import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltProcedure.VoltAbortException;

@ProcInfo(
    singlePartition = false
)

public class Insert2 extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt(
      "INSERT INTO TEST VALUES (?, ?);"
  );
  
  public final SQLStmt sql2 = new SQLStmt(
	      "SELECT * FROM TEST;"
	  );

  public VoltTable[] run( int a,
                          int b)
      throws VoltAbortException {
          voltQueueSQL( sql, a, b );
          voltQueueSQL( sql, a + 1, b );
          voltQueueSQL( sql2 );
          VoltTable[] tables = voltExecuteSQL();
          for(VoltTable t : tables) {
        	  System.out.println("the table is:" + t.toString());
          }
          try {
        	  Thread.sleep(60000);
          } catch (Exception e) {
        	  System.err.println(e);
          }
          voltQueueSQL( sql2 );
          tables = voltExecuteSQL();
          for(VoltTable t : tables) {
        	  System.out.println("the table is: " + t.toString());
          }
          return null;
      }
}
