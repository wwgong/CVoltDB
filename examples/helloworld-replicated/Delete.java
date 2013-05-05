import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltProcedure.VoltAbortException;

@ProcInfo(
    singlePartition = false
)

public class Delete extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt(
      "DELETE FROM TEST;"
  );

  public VoltTable[] run( )
      throws VoltAbortException {
          voltQueueSQL( sql );
          voltExecuteSQL();
          throw new VoltAbortException();
      }
}
