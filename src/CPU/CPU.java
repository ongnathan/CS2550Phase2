package CPU;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import data.AreaCode;
import data.IDNumber;
import data.Record;
import disk.Disk;
//import parser.Command;
import parser.DatabaseParser;
import parser.Parser;
import AfterImage.AfterImage;
import MainMemory.RowColumnStorage;
import MemoryManager.MemoryManager;

public class CPU 
{
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
			Parser scriptParser = null;
			try
			{
				scriptParser = new Parser(scriptFiles[i]);
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
					scriptParser.loadNextLine();
//					lineCounter++;
					builder.append(scriptParser.getFullString() + "\n");
				}
				catch(IOException e)
				{
					System.err.println(e.getMessage());
					builder.append(e.getMessage() + "\n");
				}
			} while(scriptParser.isError() && !scriptParser.streamIsClosed());
			
			while (!scriptParser.streamIsClosed())
			{
				switch (scriptParser.getCommand())
				{
					case READ_ID:
						value = memoryManager.readRecord(scriptParser.getTableName(), (IDNumber)scriptParser.getValue());
						result = (value != null);
						break;
					case READ_AREA_CODE:
						AreaCode area_code =  (AreaCode) scriptParser.getValue();
						value = memoryManager.readAreaCode(area_code,scriptParser.getTableName());
						result = (value != null);
						break;
					case COUNT_AREA_CODE:
						int counter = memoryManager.countAreaCode((AreaCode) scriptParser.getValue(),scriptParser.getTableName());
						result = (counter >= 0);
						value = counter;
						break;
					case INSERT:
						result=memoryManager.insertToMemory(scriptParser.getTableName(), (Record)scriptParser.getValue());
						
						break;
					case DELETE_TABLE:
						result=memoryManager.deleteTable(scriptParser.getTableName());
						break;
					default:
						throw new UnsupportedOperationException("Command not supported");
				}

				builder.append(memoryManager.getLog());
				
				if(!result)
				{
					builder.append("Failed: " + scriptParser.getFullString() + "\n");
				}
				
				switch(scriptParser.getCommand())
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
							builder.append("Inserted: " + scriptParser.getValue().toString().substring(1, scriptParser.getValue().toString().length()-1) + "\n");
							AfterImage.LogInsert(scriptParser);
						}
						
						break;
					case DELETE_TABLE:
						if(result)
						{
							builder.append("Deleted: " + scriptParser.getTableName() + "\n");
							AfterImage.LogDelete(scriptParser);
						}
						
						break;
					default:
						throw new UnsupportedOperationException("Command not supported");
				}
				
				do
				{
					try
					{
						scriptParser.loadNextLine();
//						lineCounter++;
						builder.append(scriptParser.getFullString() + "\n");
					}
					catch(IOException e)
					{
						System.err.println(e.getMessage());
						builder.append(e.getMessage() + "\n");
					}
				} while(scriptParser.isError() && !scriptParser.streamIsClosed());
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
