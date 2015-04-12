package AfterImage;

import Transaction.TransactionManager;
 
public class AfterImage {

	public static void LogInsert(TransactionManager scriptTransactionManager) {
		System.out.println("Instruction: " + scriptTransactionManager.getFullString());
		System.out.println("["+ scriptTransactionManager.getFileName()+":" + scriptTransactionManager.getLineNumber()+",I,"+scriptTransactionManager.getTableName() + "," + scriptTransactionManager.getValue()+ "]");
		
	}

	public static void LogDelete(TransactionManager scriptTransactionManager) {
		System.out.println("Instruction: " + scriptTransactionManager.getFullString());
		System.out.println("["+ scriptTransactionManager.getFileName()+":"+ scriptTransactionManager.getLineNumber()+",D,"+scriptTransactionManager.getTableName() + "]");
		
	}
}
