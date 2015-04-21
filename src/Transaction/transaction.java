package Transaction;

import parser.Command;
import data.Record;

public class transaction{
	private Command command;					//The command of the last read line
	private String tableName;					//The name of the table referenced by the last read line
	private Object value;						//The value of the last read line.  The type depends on what command is being used
	private String fullString;
	
	private int lineNumber;
	private boolean TransactionType;
	private int TID; 
	public transaction(Command command, String tableName, Object value, String fullString, int lineNumber, boolean TransactionType, int TID){
		this.command = command;
		this.tableName = tableName;
		this.value = value;
		this.fullString = fullString;
		this.lineNumber = lineNumber;
		this.TransactionType = TransactionType;
		this.TID = TID;
	}
	public Record getTinRecordFormat(){
		return new Record((String)this.value);
	}
	
	public String getFullString()
	{
		return this.fullString;
	}
	
	/**
	 * Retrieves the Command at the loaded line.
	 * @return Returns the Command that was issued by the script at the loaded line.
	 */
	public Command getCommand()
	{
		return this.command;
	}
	
	/**
	 * Retrieves the table name at the loaded line.
	 * @return Returns the String representing the table name that was issued by the script at the loaded line.
	 */
	public String getTableName()
	{
		return this.tableName;
	}
	
	/**
	 * Retrieves the record at the loaded line.
	 * @return Returns the Record representing the value that was issued by the script at the loaded line.
	 */
	public Object getValue()
	{
		return this.value;
	}
	
	/**
	 * Retrieves the line number that will be read next if loadNextLine() is called.
	 * @return Returns the integer representing the current position of the cursor in the file.
	 */
	public int getLineNumber()
	{
		return this.lineNumber;
	}
	public boolean getTransactionType()
	{
		return this.TransactionType;
	}
	public int getTID(){
		return this.TID;
	}
}