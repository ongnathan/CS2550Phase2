package parser;

/**
 * The enumerated type representing the possible commands that can be issued by a script.
 * @author Nathan Ong and Jeongmin Lee
 */
public enum Command
{
	READ_ID, READ_AREA_CODE, COUNT_AREA_CODE, INSERT, DELETE_TABLE, COMMIT, ABORT, BEGIN;
}//end enum Command
