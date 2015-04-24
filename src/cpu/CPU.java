package cpu;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import memoryManager.MemoryManager;
import memoryManager.mainMemory.RowColumnStorage;
import parser.Command;
import parser.DatabaseParser;
import scheduler.Scheduler;
import scheduler.TransactionManager;
import scheduler.Type;
import scheduler.TransactionManager.Transaction;
import data.AreaCode;
import data.IDNumber;
import data.Record;
import disk.Disk;

public class CPU
{
	public static void main(String[] args) throws IOException
	{
		int executingSequencyType = 1;
		long randomSeed = 0;
		int nextScriptIndex = 0;
		int numberOfScript = 0;
		int[] fileEndCheck;
		int endFileCount = 0;
		long debugClock = 0;
		final long timeOutSheld = 10;
		int TransactionCommitedCounter = 0;
		int writeCounter = 0;
		int readCounter = 0;
		int generalCounter = 0;
		long totalRespondTime = 0;
		int processCounter = 0;
		
		//argument check
		if(args.length < 5 || !Arrays.asList(args).contains("X"))
		{
			System.out.println("CPU Usage:\n"
					+ "java -jar CPU.jar <buffer_size_bytes> <seed> <tableName.txt> [<tableName2.txt> ...] X <script.txt> [<script2.txt> ...]");
			return;
		}
		int bufferSizeInBytes = -1;
		try
		{
			bufferSizeInBytes = Integer.parseInt(args[0]);
			randomSeed = Long.parseLong(args[1]);
		}
		catch(NumberFormatException e)
		{
			System.out.println("Error: Invalid buffer size or seed provided.");
			System.out.println("CPU Usage:\n"
					+ "java -jar CPU.jar <buffer_size_bytes> <seed> <tableName.txt> [<tableName2.txt> ...] X <script.txt> [<script2.txt> ...]");
			return;
		}
		
		if(randomSeed == 0)
		{
			executingSequencyType = 0;
		}
		
		int xLoc = Arrays.asList(args).indexOf("X");
		String[] databaseFiles = new String[xLoc-2];
		String[] scriptFiles = new String[args.length - xLoc - 1];
		for(int i = 2; i < xLoc; i++)
		{
			databaseFiles[i-2] = args[i];
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
		StringBuilder dataManagerLog = new StringBuilder();
		Scheduler scheduler = new Scheduler();
		Random rand = new Random(randomSeed);
		while(true)
		{
			//scheduler
			if(executingSequencyType == 0){//running one by one
				nextScriptIndex++;
				nextScriptIndex = nextScriptIndex%numberOfScript;
			} else {
				
				nextScriptIndex = rand.nextInt(numberOfScript);
			}
			
			boolean result = false;
			Object value = "  ";
			String afterImageLog = "";
			TransactionManager chosenTM = scriptTransactionManager[nextScriptIndex];
			do
			{
				try
				{
					++debugClock;
					chosenTM.loadNextLine();
					dataManagerLog.append(chosenTM.getFullString() + "\n");
				}
				catch(IOException e)
				{
					System.err.println(e.getMessage());
					dataManagerLog.append(e.getMessage() + "\n");
				}
			} while(chosenTM.isError() && !chosenTM.streamIsClosed());
			
			if (!chosenTM.streamIsClosed())
			{
				Transaction t = chosenTM.getTransaction();
				System.out.println(chosenTM.toString() + ":" + t.getTID());
				AreaCode areaCode = null;
				if(debugClock%timeOutSheld==0){
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
				}
				switch (chosenTM.getCommand())
				{
					case READ_ID:
						if(!scheduler.addTupleLock(Type.R, t.getTID(), (IDNumber)chosenTM.getValue(), chosenTM.getTableName(), null))
						{
							chosenTM.block();
							continue;
						}
						readCounter++;
						chosenTM.unblock();
						chosenTM.addOP();
						value = memoryManager.readRecord(chosenTM.getTableName(), (IDNumber)chosenTM.getValue());
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
						areaCode =  (AreaCode) chosenTM.getValue();
						if(!scheduler.addTupleLock(Type.M, t.getTID(), null, chosenTM.getTableName(), areaCode))
						{
							chosenTM.block();
							continue;
						}
						readCounter++;
						chosenTM.unblock();
						chosenTM.addOP();
						Record[] tempRecordList = memoryManager.readAreaCode(areaCode,chosenTM.getTableName());
						if(t.getTransactionType())
						{
							ArrayList<Record> transactionRecords = chosenTM.ReadAreaFromTempData( areaCode.areaCode, chosenTM.getTableName());
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
						areaCode =  (AreaCode) chosenTM.getValue();
						if(!scheduler.addTupleLock(Type.G, t.getTID(), null, chosenTM.getTableName(), areaCode))
						{
							chosenTM.block();
							continue;
						}
						readCounter++;
						chosenTM.unblock();
						chosenTM.addOP();
						int counter = memoryManager.countAreaCode(areaCode,chosenTM.getTableName());
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
						if(!scheduler.addTupleLock(Type.I, t.getTID(), ((Record)t.getValue()).id, chosenTM.getTableName(), null))
						{
							chosenTM.block();
							continue;
						}
						writeCounter++;
						chosenTM.unblock();
						chosenTM.addOP();
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
						if(!scheduler.addTupleLock(Type.D, t.getTID(), null, chosenTM.getTableName(), null))
						{
							chosenTM.block();
							continue;
						}
						generalCounter++;
						chosenTM.unblock();
						chosenTM.addOP();
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
						if(t.getCommand() != Command.BEGIN && t.getCommand() != Command.COMMIT && t.getCommand() != Command.ABORT)
						{
							afterImageLog = "[T_" + t.getTID() + ",BEGIN]\n" + 
									afterImageLogging(t) + 
									"[T_" + t.getTID() + ",COMMIT]\n";
							totalRespondTime += System.currentTimeMillis()-chosenTM.startTimestamp;
							processCounter++;
						}
					}
					else if(chosenTM.getCommand() == Command.COMMIT)
					{
						afterImageLog = "[T_" + t.getTID() + ",BEGIN]\n";
						for(Transaction transaction : chosenTM.getOPBuffer())
						{
							afterImageLog += afterImageLogging(transaction);
						}
						afterImageLog = afterImageLog+ 
								"[T_" + t.getTID() + ",COMMIT]\n";
						chosenTM.commit();
						long endTimestamp = System.currentTimeMillis();
						totalRespondTime += endTimestamp-chosenTM.startTimestamp;
						TransactionCommitedCounter++;
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
			} else {
				if(fileEndCheck[nextScriptIndex] == 1){
					continue;
				}
				fileEndCheck[nextScriptIndex] = 1;
				endFileCount++;
				System.out.println("Execution on " + scriptFiles[nextScriptIndex] + " is finished!");
				
				//write out data manager logs
				FileWriter writer = new FileWriter("dataManager.log", true);
				writer.append(dataManagerLog.toString());
				writer.flush();
				writer.close();
				if(endFileCount == numberOfScript){
					System.out.println(TransactionCommitedCounter +" transactions commited");
					System.out.println((double)readCounter/((double)readCounter+writeCounter+generalCounter)*100+ " % of operation is read");
					System.out.println((double)writeCounter/(double)(readCounter+writeCounter+generalCounter)*100+ " % of operation is write");
					System.out.println("Total respond time is "+ totalRespondTime);
					System.out.println("Total number of process is "+ processCounter);
					System.out.println("Average respond time is "+ totalRespondTime/(processCounter+TransactionCommitedCounter));
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
