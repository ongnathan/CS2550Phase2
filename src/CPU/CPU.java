package CPU;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import parser.DatabaseParser;
import AfterImage.AfterImage;
import MainMemory.RowColumnStorage;
import MemoryManager.MemoryManager;
import Transaction.TransactionManager;
import data.AreaCode;
import data.IDNumber;
import data.Record;
import disk.Disk;
//import parser.Parser;
//import tansactionmanager.TransactionManager;

public class CPU{

	public static void main(String[] args) throws IOException
	{
		int executingSequencyType = 0;
		int nextScriptIndex = 0;
		int numberOfScript = 0;
		int[] fileEndCheck;
		int endFileCount = 0;
		//argument check
		if(args.length < 4 || !Arrays.asList(args).contains("X"))
		{
			System.out.println("CPU Usage:\n"
					+ "java -jar CPU.jar <buffer_size_bytes> <tableName.txt> [<tableName2.txt> ...] X <script.txt> [<script2.txt> ...]");
			return;
		}
		int bufferSizeInBytes = -1;
		try
		{
			bufferSizeInBytes = Integer.parseInt(args[0]);
		}
		catch(NumberFormatException e)
		{
			System.out.println("Error: Invalid buffer size provided.");
			System.out.println("CPU Usage:\n"
					+ "java -jar CPU.jar <buffer_size_bytes> <tableName.txt> [<tableName2.txt> ...] X <script.txt> [<script2.txt> ...]");
			return;
		}
		
		int xLoc = Arrays.asList(args).indexOf("X");
		String[] databaseFiles = new String[xLoc-1];
		String[] scriptFiles = new String[args.length - xLoc - 1];
		for(int i = 1; i < xLoc; i++)
		{
			databaseFiles[i-1] = args[i];
		}
		for(int i = xLoc + 1; i < args.length; i++)
		{
			scriptFiles[i - xLoc - 1] = args[i];
		}
		
		System.out.println("myTRC! A Row and Column Store Database Simulator.");
		
		MemoryManager memoryManager = new MemoryManager(bufferSizeInBytes, new Disk(), new RowColumnStorage());
		
		//initialize all databases
		for(int i = 0; i < databaseFiles.length; i++)
		{
			String tableName = databaseFiles[i].substring(0,databaseFiles[i].length()-4);
			System.out.println("Initializing database for table " + tableName);
			DatabaseParser dbParser = null;
			try
			{
				dbParser = new DatabaseParser(databaseFiles[i]);
			}
			catch (FileNotFoundException e)
			{
				System.out.println("File " + databaseFiles[i] + " not found, skipping file.");
				continue;
			}
			for(Record r : dbParser.getRecordsFromFile())
			{
				memoryManager.insertToDisk(tableName, r);
			}
			System.out.println("Initialization for table " + tableName + " is complete");
		}
		
		//run all scripts
		TransactionManager[] scriptTransactionManager = new TransactionManager[scriptFiles.length];
		for(int i = 0; i < scriptFiles.length; i++)
		{
			//System.out.println("Running Script " + scriptFiles[i]);
			numberOfScript++;
			try
			{
				scriptTransactionManager[i] = new TransactionManager(scriptFiles[i]);
			}
			catch(FileNotFoundException e)
			{
				System.out.println("File " + scriptFiles[i] + " not found, skipping file.");
				continue;
			}
		}
		fileEndCheck = new int[numberOfScript];
		while(true){
		if(executingSequencyType == 0){//running one by one
			nextScriptIndex++;
			nextScriptIndex = nextScriptIndex%numberOfScript;
		} else {
			nextScriptIndex = ((int)Math.random()*10000)%numberOfScript;
		}
			boolean result = false;
			Object value = "  ";
			StringBuilder builder = new StringBuilder();
//			int lineCounter = 0;
			do
			{
				try
				{
					scriptTransactionManager[nextScriptIndex].loadNextLine();
//					lineCounter++;
					builder.append(scriptTransactionManager[nextScriptIndex].getFullString() + "\n");
				}
				catch(IOException e)
				{
					System.err.println(e.getMessage());
					builder.append(e.getMessage() + "\n");
				}
			} while(scriptTransactionManager[nextScriptIndex].isError() && !scriptTransactionManager[nextScriptIndex].streamIsClosed());
			
			if (!scriptTransactionManager[nextScriptIndex].streamIsClosed() && fileEndCheck[nextScriptIndex] != 1)
			{
				switch (scriptTransactionManager[nextScriptIndex].getCommand())
				{
					case READ_ID:
						result = false;
						value = memoryManager.readRecord(scriptTransactionManager[nextScriptIndex].getTableName(), (IDNumber)scriptTransactionManager[nextScriptIndex].getValue());
						if(scriptTransactionManager[nextScriptIndex].getTransaction().getTransactionType()){
							Record bufferRecord = scriptTransactionManager[nextScriptIndex].ReadIdFromTempData( ((IDNumber)scriptTransactionManager[nextScriptIndex].getValue()).value, scriptTransactionManager[nextScriptIndex].getTableName());
							result =  (bufferRecord != null );
						}
						result = (value != null) || result;
						break;
					case READ_AREA_CODE:
						AreaCode area_code =  (AreaCode) scriptTransactionManager[nextScriptIndex].getValue();
						value = memoryManager.readAreaCode(area_code,scriptTransactionManager[nextScriptIndex].getTableName());
						if(scriptTransactionManager[nextScriptIndex].getTransaction().getTransactionType()){
							ArrayList<Record> bufferRecordPhoneNumber = scriptTransactionManager[nextScriptIndex].ReadAreaFromTempData( area_code.areaCode, scriptTransactionManager[nextScriptIndex].getTableName());
							//TODO: Convert to object value, type issue 
							//bufferRecordPhoneNumber.addAll( new ArrayList<Record>(Arrays.asList((Record)value)));
							value = bufferRecordPhoneNumber.toArray();
						}
						result = (value != null);
						break;
					case COUNT_AREA_CODE:
						int counter = memoryManager.countAreaCode((AreaCode) scriptTransactionManager[nextScriptIndex].getValue(),scriptTransactionManager[nextScriptIndex].getTableName());
						if(scriptTransactionManager[nextScriptIndex].getTransaction().getTransactionType()){
							counter += scriptTransactionManager[nextScriptIndex].CountFromTempData((AreaCode) scriptTransactionManager[nextScriptIndex].getValue(), scriptTransactionManager[nextScriptIndex].getTableName());
						}
						result = (counter >= 0);
						value = counter;
						break;
					case INSERT:
						if(scriptTransactionManager[nextScriptIndex].getTransaction().getTransactionType()){
							result = scriptTransactionManager[nextScriptIndex].writeToTempData(scriptTransactionManager[nextScriptIndex].getTransaction().getTinRecordFormat(),scriptTransactionManager[nextScriptIndex].getTransaction().getTableName());
						}
						else{
							result=memoryManager.insertToMemory(scriptTransactionManager[nextScriptIndex].getTableName(), (Record)scriptTransactionManager[nextScriptIndex].getValue());
						}
						break;
					case DELETE_TABLE:
						//TODO: delete buffer needed, since we need to store delete state
						if(!scriptTransactionManager[nextScriptIndex].getTransaction().getTransactionType()){
							result=memoryManager.deleteTable(scriptTransactionManager[nextScriptIndex].getTableName());
						}else{
							//put into op buffer
						}
						break;
					case COMMIT:	// need to check commit function in MM
						if(scriptTransactionManager[nextScriptIndex].getTransaction().getTransactionType()){
							result=memoryManager.commitToTransaction(scriptTransactionManager[nextScriptIndex].getOPBuffer());
						}
						break;
					case ABORT:
						if(scriptTransactionManager[nextScriptIndex].getTransaction().getTransactionType()){
							scriptTransactionManager[nextScriptIndex].Abort();
						}
					default:
						throw new UnsupportedOperationException("Command not supported");
				}

				builder.append(memoryManager.getLog());
				
				if(!result)
				{
					builder.append("Failed: " + scriptTransactionManager[nextScriptIndex].getFullString() + "\n");
				}
				
				switch(scriptTransactionManager[nextScriptIndex].getCommand())
				{
					case READ_ID:
						if(result)
						{
							builder.append("Read: " + value.toString().substring(1, value.toString().length()-1) + "\n");
						}
						break;
					case READ_AREA_CODE:
						if(result)
						{
							for(Record r : (Record []) value)
							{
								builder.append("MRead: " + r.toString().substring(1, r.toString().length()-1) + "\n");
							}
						}
						break;
					case COUNT_AREA_CODE:
						if(result)
						{
							builder.append("GCount: " + (int)value + "\n");
						}
						break;
					case INSERT:
						if(result)
						{
							builder.append("Inserted: " + scriptTransactionManager[nextScriptIndex].getValue().toString().substring(1, scriptTransactionManager[nextScriptIndex].getValue().toString().length()-1) + "\n");
							AfterImage.LogInsert(scriptTransactionManager[nextScriptIndex]);
						}
						
						break;
					case DELETE_TABLE:
						if(result)
						{
							builder.append("Deleted: " + scriptTransactionManager[nextScriptIndex].getTableName() + "\n");
							AfterImage.LogDelete(scriptTransactionManager[nextScriptIndex]);
						}
					case COMMIT:		// need to check this
						if(result)
						{
							builder.append("Commit: " + scriptTransactionManager[nextScriptIndex].getOPBuffer() + "\n" );
						}
					case ABORT:			// need to check this
						if(result)
						{
							builder.append("Abort: " + "\n");
							AfterImage.LogInsert(scriptTransactionManager[nextScriptIndex]);
						}
						
						break;
					default:
						throw new UnsupportedOperationException("Command not supported");
				}
				
				do
				{
					try
					{
						scriptTransactionManager[nextScriptIndex].loadNextLine();
//						lineCounter++;
						builder.append(scriptTransactionManager[nextScriptIndex].getFullString() + "\n");
					}
					catch(IOException e)
					{
						System.err.println(e.getMessage());
						builder.append(e.getMessage() + "\n");
					}
				} while(scriptTransactionManager[nextScriptIndex].isError() && !scriptTransactionManager[nextScriptIndex].streamIsClosed());
			} else {
				fileEndCheck[nextScriptIndex] = 1;
				endFileCount++;
				System.out.println("Execution on " + scriptFiles[nextScriptIndex] + " is finished!");
				System.out.println("Dump:");
				System.out.println(builder.toString());
				
				FileWriter writer = new FileWriter(scriptFiles[nextScriptIndex].substring(0, scriptFiles[nextScriptIndex].length() - 4) + ".log");
				writer.append(builder.toString());
				writer.flush();
				writer.close();
				if(endFileCount == numberOfScript){
					System.exit(0);
				}
			}
		}
	}
	
}
