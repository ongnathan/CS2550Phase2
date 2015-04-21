package Scheduler;

import java.awt.Graphics;
import java.util.ArrayList;
import java.util.Set;

public class Scheduler {
	ArrayList<Lock> LockTable = new ArrayList<Lock>();
	long timestamp;
	
	public Scheduler() {
		timestamp = 0;
	}
	
	public void addTupleLock(Type type, int TID, int ID, String TableName, String AreaCode){
		Lock l = new Lock(type, 0, TID, ID, TableName, AreaCode);
		addLock(l);
		return ;
	}
	
	private void addLock(Lock L){
		L.WaitforT = getLatestWaitfor(L);
		LockTable.add(L);
		return ;
	}
	
	public void reaseLock(int TID){
		int i = LockTable.size();
		while( i >= 0 ){
			if( LockTable.get(i).TID == TID )
				LockTable.remove(i);
			if( LockTable.get(i).WaitforT == TID )
				LockTable.get(i).WaitforT = -1;
			--i;
		}
		return ;
	}
	
	private int getLatestWaitfor(Lock L){
		int i = LockTable.size();
		while( i >= 0 ){
			if(CompatTable(L.Ttype,LockTable.get(i).Ttype)){
					--i;
					continue;
			}else{
					if( L.TableName.equals(LockTable.get(i).TableName) &&  (( L.ID == LockTable.get(i).ID ) || L.AreaCode.equals(LockTable.get(i).AreaCode) ))
					{
						return LockTable.get(i).TID;
					}
					else{
						--i;
					}
			}
		}
		return -1;
	}

	private Set<Integer> CycleDectact(){
		Graph WaitforGraph = new Graph();
		int i = LockTable.size();
		while( i >= 0 ){
			WaitforGraph.addEdge(LockTable.get(i).TID, LockTable.get(i).WaitforT);
			--i;
		} 
		return WaitforGraph.checkCycle();
	}
	
	public void DeadLockDetectFree(){
		Set<Integer> DeadVertex = CycleDectact();
		if(!DeadVertex.isEmpty()){
			int DeadVertexTID = DeadVertex.iterator().next();
			//get dead TID, then clear the table entry whose wait for field is this TID
			reaseLock(DeadVertexTID);
		}
		return ;
	}
	
	private boolean CompatTable(Type T1, Type T2){
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
		
		public Lock(Type type, int Stamp, int TID, int ID, String TableName, String AreaCode){
			this.Ttype = type;
			this.TimeStamp = Stamp;
			this.TID = TID;
			this.ID = ID;
			this.TableName = TableName;
			this.AreaCode = AreaCode;
			this.WaitforT = -1;
		}
		
	}
}
