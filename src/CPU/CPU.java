package CPU;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import parser.Command;
import parser.DatabaseParser;
import MainMemory.RowColumnStorage;
import MemoryManager.MemoryManager;
import Scheduler.Scheduler;
import Scheduler.Type;
import Transaction.TransactionManager;
import Transaction.TransactionManager.Transaction;
import data.AreaCode;
import data.IDNumber;
import data.Record;
import disk.Disk;
//import parser.Parser;
//import tansactionmanager.TransactionManager;

public class CPU
{
	public static void main(String[] args) throws IOException
	{
		int executingSequencyType = 0;
		int nextScriptIndex = 0;
		int numberOfScript = 0;
		int[] fileEndCheck;
		int endFileCount = 0;
		int debugCount = 0;
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
		
		Scheduler scheduler = new Scheduler();
		while(true)
		{
			//scheduler
			if(executingSequencyType == 0){//running one by one
				nextScriptIndex++;
				nextScriptIndex = nextScriptIndex%numberOfScript;
			} else {
				nextScriptIndex = ((int)Math.random()*10000)%numberOfScript;
			}
			++debugCount;
			boolean result = false;
			Object value = "  ";
			StringBuilder dataManagerLog = new StringBuilder();
			String afterImageLog = "";
			TransactionManager chosenTM = scriptTransactionManager[nextScriptIndex];
			do
			{
				try
				{
					chosenTM.loadNextLine();
					dataManagerLog.append(chosenTM.getFullString() + "\n");
				}
				catch(IOException e)
				{
					System.err.println(e.getMessage());
					dataManagerLog.append(e.getMessage() + "\n");
				}
			} while(chosenTM.isError() && !chosenTM.streamIsClosed());
			
			if (!chosenTM.streamIsClosed() && fileEndCheck[nextScriptIndex] != 1)
			{
				Transaction t = chosenTM.getTransaction();
				switch (chosenTM.getCommand())
				{
					case READ_ID:
						result = false;
						value = memoryManager.readRecord(chosenTM.getTableName(), (IDNumber)chosenTM.getValue());
						if(value!=null){
							scheduler.addTupleLock(Type.R, t.getTID(), ((Record)value).id, chosenTM.getTableName(), null);
						}
						if(t.getTransactionType() && value == null)
						{
							value = chosenTM.ReadIdFromTempData(((IDNumber)chosenTM.getValue()).value, chosenTM.getTableName());
						}
						else if(!t.getTransactionType())
						{
							scheduler.releaseLock(t.getTID());
						}
						result = (value != null);
						break;
					case READ_AREA_CODE:
						AreaCode area_code =  (AreaCode) chosenTM.getValue();
						Record[] tempRecordList = memoryManager.readAreaCode(area_code,chosenTM.getTableName());
						scheduler.addTupleLock(Type.M, t.getTID(), null, chosenTM.getTableName(), area_code);
						if(t.getTransactionType())
						{
							ArrayList<Record> transactionRecords = chosenTM.ReadAreaFromTempData( area_code.areaCode, chosenTM.getTableName());
							if(tempRecordList != null)
							{
								transactionRecords.addAll(Arrays.asList(tempRecordList));
							}
							value = transactionRecords.isEmpty() ? null : transactionRecords.toArray(new Record[0]);
						}
						else
						{
							value = tempRecordList;
							scheduler.releaseLock(t.getTID());
						}
						result = (value != null);
						break;
					case COUNT_AREA_CODE:
						AreaCode areaCode =  (AreaCode) chosenTM.getValue();
						int counter = memoryManager.countAreaCode(areaCode,chosenTM.getTableName());
						scheduler.addTupleLock(Type.G, t.getTID(), null, chosenTM.getTableName(), areaCode);
						if(t.getTransactionType())
						{
							counter += chosenTM.CountFromTempData(areaCode, chosenTM.getTableName());
						}
						else
						{
							scheduler.releaseLock(t.getTID());
						}
						result = (counter >= 0);
						value = counter;
						break;
					case INSERT:
						scheduler.addTupleLock(Type.I, t.getTID(), ((Record)t.getValue()).id, chosenTM.getTableName(), null);
						if(t.getTransactionType())
						{
							result = chosenTM.writeToTempData(t.getTinRecordFormat(),chosenTM.getTransaction().getTableName());
						}
						else
						{
							result=memoryManager.insertToMemory(chosenTM.getTableName(), (Record)chosenTM.getValue());
							scheduler.releaseLock(t.getTID());
						}
						break;
					case DELETE_TABLE:
						scheduler.addTupleLock(Type.D, t.getTID(), null, chosenTM.getTableName(), null);
						if(t.getTransactionType())
						{
							//put into op buffer
							result = true;
						}
						else
						{
							result=memoryManager.deleteTable(chosenTM.getTableName());
							scheduler.releaseLock(t.getTID());
						}
						break;
					case COMMIT:	// need to check commit function in MM
						if(t.getTransactionType())
						{
							result=memoryManager.commitToTransaction(chosenTM.getOPBuffer());
							scheduler.releaseLock(t.getTID());
						}
						else
						{
							result = true;
						}
						break;
					case ABORT:
						if(t.getTransactionType())
						{
							chosenTM.Abort();
							scheduler.releaseLock(t.getTID());
						}
						result = true;
						break;
					case BEGIN:
						//Nothing
						result = true;
						break;
					default:
						throw new UnsupportedOperationException("Command not supported");
				}
				
				dataManagerLog.append(memoryManager.getLog());
				
				if(!result)
				{
					dataManagerLog.append("Failed: " + chosenTM.getFullString() + "\n");
				}
				else
				{
					//for afterImageLog
					if(!t.getTransactionType())
					{
						afterImageLog = "[T_" + t.getTID() + ", BEGIN]\n" + 
								afterImageLogging(t) + 
								"[T_" + t.getTID() + ", COMMIT]\n";
					}
					else if(chosenTM.getCommand() == Command.COMMIT)
					{
						for(Transaction transaction : chosenTM.getOPBuffer())
						{
							afterImageLog += afterImageLogging(transaction);
						}
					}
					
					if(!afterImageLog.isEmpty())
					{
						FileWriter writer = new FileWriter("afterImage.log", true);
						writer.append(afterImageLog);
						writer.flush();
						writer.close();
						
						afterImageLog = "";
					}
					
					//for dataManagerLog
					switch(chosenTM.getCommand())
					{
						case READ_ID:
							dataManagerLog.append("Read: " + value.toString().substring(1, value.toString().length()-1) + "\n");
							break;
						case READ_AREA_CODE:
							for(Record r : (Record []) value)
							{
								dataManagerLog.append("MRead: " + r.toString().substring(1, r.toString().length()-1) + "\n");
							}
							break;
						case COUNT_AREA_CODE:
							dataManagerLog.append("GCount: " + (int)value + "\n");
							break;
						case INSERT:
							dataManagerLog.append("Inserted: " + chosenTM.getValue().toString().substring(1, chosenTM.getValue().toString().length()-1) + "\n");
							break;
						case DELETE_TABLE:
							dataManagerLog.append("Deleted: " + chosenTM.getTableName() + "\n");
							break;
						case COMMIT:		// need to check this
						case ABORT:			// need to check this
						case BEGIN:
							dataManagerLog.append(chosenTM.getCommand().name() + ": " + chosenTM.getTransaction().getTID() + "\n" );
							break;
						default:
							throw new UnsupportedOperationException("Command not supported");
					}
				}
				
				//check for deadlock
				int deadLockedTID = scheduler.DeadLockDetectFree();
				if(deadLockedTID != -1)
				{
					boolean assertionCheck = false;
					for(TransactionManager manager : scriptTransactionManager)
					{
						if(manager.getTransaction().getTID() == deadLockedTID)
						{
							assertionCheck = true;
							manager.DeadLockAbort();
							
							//write out to after image
							FileWriter writer = new FileWriter("afterImage.log", true);
							writer.append("[T_" + t.getTID() + ", DEADLOCK ABORT]\n");
							writer.flush();
							writer.close();
							break;
						}
					}
					if(!assertionCheck)
					{
						throw new IllegalStateException("Could not find transaction to abort; deadlock still detected.");
					}
				}
				
			} else {
				fileEndCheck[nextScriptIndex] = 1;
				endFileCount++;
				System.out.println("Execution on " + scriptFiles[nextScriptIndex] + " is finished!");
				
				//write out data manager logs
				FileWriter writer = new FileWriter("dataManager.log", true);
				writer.append(dataManagerLog.toString());
				writer.flush();
				writer.close();
				if(endFileCount == numberOfScript){
					System.exit(0);
				}
			}
		}
	}
	
	public static String afterImageLogging(Transaction t)
	{
		switch(t.getCommand())
		{
			case READ_ID:
			case READ_AREA_CODE:
			case COUNT_AREA_CODE:
				return "";
			case INSERT:
				return "[T_" + t.getTID() + "," + t.getTableName() + ":" + ((Record)t.getValue()).id + "," + t.getValue().toString() + "]\n";
			case DELETE_TABLE:
				return "[T_" + t.getTID() + "," + t.getTableName() + "," + "DELETED" + "]\n";
			case BEGIN:
			case COMMIT:
			case ABORT:
				return "[T_" + t.getTID() + "," + t.getCommand().name() + "]\n";
			default:
				throw new UnsupportedOperationException("Command not supported");
		}
	}
}
