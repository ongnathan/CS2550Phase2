package AfterImage;

import parser.Command;
import parser.Parser;

public class AfterImage {

	public static void LogInsert(Parser scriptParser) {
		System.out.println("Instruction: " + scriptParser.getFullString());
		System.out.println("["+ scriptParser.getFileName()+":" + scriptParser.getLineNumber()+",I,"+scriptParser.getTableName() + "," + scriptParser.getValue()+ "]");
		
	}

	public static void LogDelete(Parser scriptParser) {
		System.out.println("Instruction: " + scriptParser.getFullString());
		System.out.println("["+ scriptParser.getFileName()+":"+ scriptParser.getLineNumber()+",D,"+scriptParser.getTableName() + "]");
		
	}
}
