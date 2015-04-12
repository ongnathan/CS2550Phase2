package CPU;

import java.io.FileNotFoundException;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

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
		for(int i = 0; i < scriptFiles.length; i++)
		{
			System.out.println("Running Script " + scriptFiles[i]);
		
			TransactionManager scriptTransactionManager = null;
			try
			{
				scriptTransactionManager = new TransactionManager(scriptFiles[i]);
			}
			catch(FileNotFoundException e)
			{
				System.out.println("File " + scriptFiles[i] + " not found, skipping file.");
				continue;
			}
			
			boolean result = false;
			Object value = "  ";
			StringBuilder builder = new StringBuilder();
//			int lineCounter = 0;
			do
			{
				try
				{
					scriptTransactionManager.loadNextLine();
//					lineCounter++;
					builder.append(scriptTransactionManager.getFullString() + "\n");
				}
				catch(IOException e)
				{
					System.err.println(e.getMessage());
					builder.append(e.getMessage() + "\n");
				}
			} while(scriptTransactionManager.isError() && !scriptTransactionManager.streamIsClosed());
			
			while (!scriptTransactionManager.streamIsClosed())
			{
				switch (scriptTransactionManager.getCommand())
				{
					case READ_ID:
						value = memoryManager.readRecord(scriptTransactionManager.getTableName(), (IDNumber)scriptTransactionManager.getValue());
						result = (value != null);
						break;
					case READ_AREA_CODE:
						AreaCode area_code =  (AreaCode) scriptTransactionManager.getValue();
						value = memoryManager.readAreaCode(area_code,scriptTransactionManager.getTableName());
						result = (value != null);
						break;
					case COUNT_AREA_CODE:
						int counter = memoryManager.countAreaCode((AreaCode) scriptTransactionManager.getValue(),scriptTransactionManager.getTableName());
						result = (counter >= 0);
						value = counter;
						break;
					case INSERT:
						result=memoryManager.insertToMemory(scriptTransactionManager.getTableName(), (Record)scriptTransactionManager.getValue());
						break;
					case DELETE_TABLE:
						result=memoryManager.deleteTable(scriptTransactionManager.getTableName());
						break;
					case COMMIT:	// need to check commit function in MM
						result=memoryManager.commitToTransaction(scriptTransactionManager.getOPBuffer());
						break;
					case ABORT:
						scriptTransactionManager.Abort();
					default:
						throw new UnsupportedOperationException("Command not supported");
				}

				builder.append(memoryManager.getLog());
				
				if(!result)
				{
					builder.append("Failed: " + scriptTransactionManager.getFullString() + "\n");
				}
				
				switch(scriptTransactionManager.getCommand())
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
							builder.append("Inserted: " + scriptTransactionManager.getValue().toString().substring(1, scriptTransactionManager.getValue().toString().length()-1) + "\n");
							AfterImage.LogInsert(scriptTransactionManager);
						}
						
						break;
					case DELETE_TABLE:
						if(result)
						{
							builder.append("Deleted: " + scriptTransactionManager.getTableName() + "\n");
							AfterImage.LogDelete(scriptTransactionManager);
						}
					case COMMIT:		// need to check this
						if(result)
						{
							builder.append("Commit: " + scriptTransactionManager.getOPBuffer() + "\n" );
						}
					case ABORT:			// need to check this
						if(result)
						{
							builder.append("Abort: " + "\n");
							AfterImage.LogInsert(scriptTransactionManager);
						}
						
						break;
					default:
						throw new UnsupportedOperationException("Command not supported");
				}
				
				do
				{
					try
					{
						scriptTransactionManager.loadNextLine();
//						lineCounter++;
						builder.append(scriptTransactionManager.getFullString() + "\n");
					}
					catch(IOException e)
					{
						System.err.println(e.getMessage());
						builder.append(e.getMessage() + "\n");
					}
				} while(scriptTransactionManager.isError() && !scriptTransactionManager.streamIsClosed());
			}
			System.out.println("Execution on " + scriptFiles[i] + " is finished!");
			System.out.println("Dump:");
			System.out.println(builder.toString());
			
			FileWriter writer = new FileWriter(scriptFiles[i].substring(0, scriptFiles[i].length() - 4) + ".log");
			writer.append(builder.toString());
			writer.flush();
			writer.close();
		}
	}
	
}
