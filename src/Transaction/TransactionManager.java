package Transaction;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;

import parser.Command;
import data.AreaCode;
import data.IDNumber;
import data.PhoneNumber;
import data.Record;

public class TransactionManager {
	private final File file;
	private final BufferedReader fileReader;	//The file reader
	
	private Command command;					//The command of the last read line
	private String tableName;					//The name of the table referenced by the last read line
	private Object value;						//The value of the last read line.  The type depends on what command is being used
	private String fullString;
	
	private int lineNumber;						//The current line number in the script
	private boolean streamIsClosed;				//Whether or not the stream has been closed due to reaching the end of file or not
	
	private boolean error;
	private boolean TransactionType;
	private int TID; 
	private ArrayList<transaction> OPBuffer;
	private ArrayList<ArrayList<Record>> tempData;
	private ArrayList<String> tempTableIndex;//works to find the index of the table in the tempdata
	
	/**
	 * The constructor.
	 * Takes in a file name representing the location to find the script.
	 * @param filename The String representing the file name.
	 * @throws FileNotFoundException if the file does not exist.
	 */
	public TransactionManager(String filename) throws FileNotFoundException
	{
		this(new File(filename));
	}
	
	/**
	 * The constructor.
	 * Takes in a File representing the script.
	 * @param file The File to read from.
	 * @throws FileNotFoundException if the file does not exist.
	 */
	public TransactionManager(File file) throws FileNotFoundException
	{
		this.file = file;
		this.fileReader = new BufferedReader(new FileReader(file));
		this.lineNumber = 0;
		this.streamIsClosed = false;
		this.error = false;
		this.TransactionType = false;
		this.TID = -1;
		this.OPBuffer = new ArrayList<transaction>();
		this.tempData = new ArrayList<ArrayList<Record>>();
		this.tempTableIndex = new ArrayList<String>();
	}
	
	/**
	 * Loads the next line of the script.
	 * @throws IOException if the end of the file has been reached or if the script has a malformed line.
	 */
	public void loadNextLine() throws IOException
	{
		this.error = false;
		if(this.streamIsClosed)
		{
			this.error = true;
			throw new EOFException("The end of the file has been reached.");
		}
		
		String line = this.fileReader.readLine();
		if(line == null)
		{
			this.fullString = "";
			this.command = null;
			this.tableName = null;
			this.value = null;
			this.streamIsClosed = true;
			this.fileReader.close();
			return;
		}

		this.lineNumber++;
		
		String[] split = line.split(" ");
		if(split.length > 3)
		{
			this.error = true;
			throw new IOException("File is not in the correct format at line " + this.lineNumber + ".");
		}
		else if(split.length == 1){
			if(split[0].equals("C")){
				this.command = Command.COMMIT;
			} else if(split[0].equals("A")){
				this.command = Command.ABORT;
			} else {
				this.error = true;
				throw new IOException("File is not in the correct format at line " + this.lineNumber + ".");
			}
		}
		else if(split.length == 2)
		{
			if(!split[0].equals("D") || !split[0].equals("B"))
			{
				this.error = true;
				throw new IOException("File is not in the correct format at line " + this.lineNumber + ".");
//				this.loadNextLine();
//				this.error = "Malformed command at line " + this.lineNumber + "of the script.\n"
//						+ "Skipping that line.";
//				return;
			} else if(split[0].equals("D")){
				this.command = Command.DELETE_TABLE;
				this.tableName = split[1];
				this.value = null;
			} else if(split[0].equals("B")){
				this.command = Command.BEGIN;
				this.value = Integer.getInteger(split[1]);
				if((int)value == 1){
					TransactionType = true;
					TID = 1; // when cpu finished we add it here;
				} else {
					TransactionType = false;
					TID = -1;
				}
				
			}
		}
		else
		{
			switch(split[0])
			{
				case "R":
					this.command = Command.READ_ID;
					this.value = new IDNumber(split[2]);
					break;
				case "M":
					this.command = Command.READ_AREA_CODE;
					this.value = new AreaCode(split[2]);
					break;
				case "G":
					this.command = Command.COUNT_AREA_CODE;
					this.value = new AreaCode(split[2]);
					break;
				case "I":
					this.command = Command.INSERT;
					this.value = new Record(split[2]);
					break;
				default:
					this.error = true;
					throw new IOException("File is not in the correct format " + this.lineNumber + ".");
			}//end switch(String)
			this.tableName = split[1];
		}//end else
		this.fullString = line;
	}//end method()
	
	public ArrayList<transaction> getOPBuffer(){
		ArrayList<transaction> temp = (ArrayList<transaction>) OPBuffer.clone();
		OPBuffer.clear();
		tempData.clear();
		tempTableIndex.clear();
		TID = -1;
		return temp;
		
	}
	public boolean writeToTempData(Record r,String tableName){
		for(int i = 0; i < tempData.size(); i ++){
			if(tempTableIndex.get(i).equals(tableName)){
				tempData.get(i).add(r);
				return true;
			}
		}
		tempTableIndex.add(tableName);
		ArrayList<Record> tempArray = new ArrayList<Record>();
		tempArray.add(r);
		tempData.add(tempArray);
		return true;
	}
	
	public Record ReadIdFromTempData(long id, String tableName){
		if(ifDeletetable(tableName)) 
			return null;
		for(int i = 0; i < tempData.size(); i ++){
			if(tempTableIndex.get(i).equals(tableName)){
				for(int j= 0; j < tempData.get(i).size();j++){
					if(tempData.get(i).get(j).id.value == id){
						return tempData.get(i).get(j);
					}
				}
			}
		}
		return null;
	}
	public ArrayList<Record> ReadAreaFromTempData(String area, String tableName){
		if(ifDeletetable(tableName)) 
			return null;
		ArrayList<Record> tempReturnResult = new ArrayList<Record>();
		for(int i = 0; i < tempData.size(); i ++){
			if(tempTableIndex.get(i).equals(tableName)){
				for(int j= 0; j < tempData.get(i).size();j++){
					if(tempData.get(i).get(j).phoneNumber.areaCode.equals(area)){
						tempReturnResult.add(tempData.get(i).get(j));
					}
				}
			}
		}
		return tempReturnResult;
	}
	public int CountFromTempData(PhoneNumber pNumber, String tableName){
		if(ifDeletetable(tableName)) 
			return 0;
		int tempReturnResult = 0;
		for(int i = 0; i < tempData.size(); i ++){
			if(tempTableIndex.get(i).equals(tableName)){
				for(int j= 0; j < tempData.get(i).size();j++){
					if(tempData.get(i).get(j).phoneNumber.areaCode.equals(pNumber.areaCode)){
						tempReturnResult++;
					}
				}
			}
		}
		return tempReturnResult;
	}
	
	private boolean ifDeletetable(String tableName){
		for(transaction op : OPBuffer){
			if( op.getCommand()==Command.DELETE_TABLE&& op.getTableName()==tableName){
				return true;
			}
		}
		return false;
	}
	
	public void Abort(){
		TransactionType = false;
		TID = -1;
		OPBuffer.clear();
		tempData.clear();
		tempTableIndex.clear();
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

	//Added by Jeongmin
	public boolean streamIsClosed()
	{
		return this.streamIsClosed;
	}
	
	public boolean isError()
	{
		return this.error;
	}
	
	public String getFileName()
	{
		return this.file.getName();
	}
	public transaction getTransaction(){
		
		transaction temp =  new transaction(command,tableName,value,fullString,lineNumber,TransactionType,TID);
		if(TransactionType){
			OPBuffer.add(temp);
		}
		return temp;
	}
	
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
}
