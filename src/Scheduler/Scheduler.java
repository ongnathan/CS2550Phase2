package Scheduler;

import java.util.ArrayList;

public class Scheduler {
	ArrayList<Lock> LockTable = new ArrayList<Lock>();
	long timestamp;
	
	public Scheduler() {
		timestamp = 0;
	}
	
	public void AddLock(Lock L){
		LockTable.add(L);
		return ;
	}
	
	public int getLatestWaitfor(Lock L){
		int i;
		for(i = LockTable.size();(i>=0) && CompatTable(L.Ttype,LockTable.get(i).Ttype);--i);
		return LockTable.get(i).TID;
	}
	
	public boolean CompatTable(Type T1, Type T2){
		if( T1==Type.R || T1==Type.M || T1==Type.R){
			if( T2==Type.R || T2==Type.M || T2==Type.R){
				return true;
			}
		}
		return false;
	}
	
	
	public class Lock{
		public Type Ttype;
		public long TimeStamp;
		public int TID;
		public int ID;//Record ID
		public String TableName;
		public String AreaCode;
		public int WaitforT;
		
		public Lock(Type type, int Stamp, int TID, int ID, String TableName, String AreaCode, int WaitforT){
			this.Ttype = type;
			this.TimeStamp = Stamp;
			this.TID = TID;
			this.ID = ID;
			this.TableName = TableName;
			this.AreaCode = AreaCode;
			this.WaitforT = WaitforT;
		}
		
	}
}
