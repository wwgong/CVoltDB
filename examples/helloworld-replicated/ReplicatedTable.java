
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltProcedure.VoltAbortException;

@ProcInfo(
    singlePartition = false
)

public class ReplicatedTable extends VoltProcedure {

  public final SQLStmt insertuser = new SQLStmt(
      "INSERT INTO USERACCOUNT VALUES (?,?,?,?,?);"
  );
  
  public final SQLStmt getuser = new SQLStmt(
	      "SELECT H.HELLO, U.FIRSTNAME " +
	      "FROM USERACCOUNT AS U, HELLOWORLD AS H " +
	      "WHERE U.EMAIL = ? AND U.DIALECT = H.DIALECT;"
	  );
  
  public final SQLStmt updatesignin = new SQLStmt(
	      "UPDATE USERACCOUNT SET lastlogin=? " +
	      "WHERE EMAIL = ?;"
	  );
  
  public final SQLStmt getHello = new SQLStmt(
		  "SELECT * FROM HELLOWORLD;");
  
  public final SQLStmt insertNewHello = new SQLStmt(
		  "INSERT INTO HELLOWORLD VALUES (?, ?, ?);"
		  );

  public VoltTable[] run( String email, String firstname, 
                   String lastname, String language)
      throws VoltAbortException {

          voltQueueSQL( insertuser, email, firstname, 
                        lastname, null, language);
          voltQueueSQL( insertNewHello, "Nihao", "Shijie", "Chinese" );
          voltQueueSQL( getuser, EXPECT_ONE_ROW, email );
          voltQueueSQL( updatesignin,  System.currentTimeMillis(), email );
          voltQueueSQL( getHello );
          voltQueueSQL( getuser, EXPECT_ONE_ROW, email );
          return voltExecuteSQL();
      }
}