package parser;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import data.AreaCode;
import data.IDNumber;
import data.Record;

/**
 * The script parser.
 * Reads the script one line at a time.
 * The line can be accessed as many times as necessary.
 * When a new line is read, the previous line is discarded.
 * @author Nathan Ong and Jeongmin Lee
 */
public class Parser
{
	private final File file;
	private final BufferedReader fileReader;	//The file reader
	
	private Command command;					//The command of the last read line
	private String tableName;					//The name of the table referenced by the last read line
	private Object value;						//The value of the last read line.  The type depends on what command is being used
	private String fullString;
	
	private int lineNumber;						//The current line number in the script
	private boolean streamIsClosed;				//Whether or not the stream has been closed due to reaching the end of file or not
	
	private boolean error;
	
	/**
	 * The constructor.
	 * Takes in a file name representing the location to find the script.
	 * @param filename The String representing the file name.
	 * @throws FileNotFoundException if the file does not exist.
	 */
	public Parser(String filename) throws FileNotFoundException
	{
		this(new File(filename));
	}
	
	/**
	 * The constructor.
	 * Takes in a File representing the script.
	 * @param file The File to read from.
	 * @throws FileNotFoundException if the file does not exist.
	 */
	public Parser(File file) throws FileNotFoundException
	{
		this.file = file;
		this.fileReader = new BufferedReader(new FileReader(file));
		this.lineNumber = 0;
		this.streamIsClosed = false;
		this.error = false;
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
		if(split.length < 2 || split.length > 3)
		{
			this.error = true;
			throw new IOException("File is not in the correct format at line " + this.lineNumber + ".");
		}
		else if(split.length == 2)
		{
			if(!split[0].equals("D"))
			{
				this.error = true;
				throw new IOException("File is not in the correct format at line " + this.lineNumber + ".");
//				this.loadNextLine();
//				this.error = "Malformed command at line " + this.lineNumber + "of the script.\n"
//						+ "Skipping that line.";
//				return;
			}
			this.command = Command.DELETE_TABLE;
			this.tableName = split[1];
			this.value = null;
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
}//end class Parser
