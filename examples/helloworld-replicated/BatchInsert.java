import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltProcedure.VoltAbortException;

@ProcInfo(
    singlePartition = false
)

public class BatchInsert extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt(
      "INSERT INTO HELLOWORLD VALUES (?, ?, ?);"
  );

  public VoltTable[] run()
      throws VoltAbortException {
          voltQueueSQL( sql, "Hello", "World", "English" );
          voltQueueSQL( sql, "Bonjour", "Monde", "French");
          voltQueueSQL( sql, "Hola", "Mundo", "Spanish");
          voltQueueSQL( sql, "Hej", "Verden", "Danish");
          voltQueueSQL( sql, "Ciao", "Mondo", "Italian");
          voltExecuteSQL();
          return null;
      }
}
