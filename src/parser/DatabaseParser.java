package parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import data.Record;

public class DatabaseParser
{
	private final BufferedReader fileReader;	//The file reader
	
	/**
	 * The constructor.
	 * it takes the file name that represents the location to find the script.
	 * @param filename the file name to be parsed.
	 * @throws FileNotFoundException if no file.
	 */
	
	public DatabaseParser(String filename) throws FileNotFoundException
	{
		this(new File(filename));
	}
	
	public DatabaseParser(File file) throws FileNotFoundException
	{
		this.fileReader = new BufferedReader(new FileReader(file));
	}
	
	/**
	 * this is to read the record from a given file 
	 * @return the list of the record in a file 
	 * @throws IOException if no records.
	 */
	
	public Record[] getRecordsFromFile() throws IOException
	{
		ArrayList<Record> recordList = new ArrayList<Record>();
		String line = this.fileReader.readLine();
		while(line != null)
		{
			recordList.add(new Record("(" + line + ")"));
			line = this.fileReader.readLine();
		}
		this.fileReader.close();
		return recordList.toArray(new Record[0]);
	}
}
