import org.voltdb.*;

@ProcInfo(
    singlePartition = false,
    limitedToPartition = true,
    partitionInfo = "TEST.A:1,2"
)

public class Select extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt(
      "SELECT HELLO, WORLD FROM HELLOWORLD " +
      " WHERE DIALECT = ?;"
  );
  
  public final SQLStmt sql2 = new SQLStmt(
		  "SELECT * FROM TEST WHERE A = ? OR A = ?");

  public VoltTable[] run( String language, int i, int j)
      throws VoltAbortException {
          voltQueueSQL( sql, language );
          return voltExecuteSQL();
      }
}
