package Scheduler;

import java.awt.Graphics;
import java.util.ArrayList;
import java.util.Set;

import data.AreaCode;
import data.IDNumber;

public class Scheduler {
	ArrayList<Lock> LockTable = new ArrayList<Lock>();
	long timestamp;
	
	public Scheduler() {
		timestamp = 0;
	}
	
	//use
	public void addTupleLock(Type type, int TID, IDNumber id, String TableName, AreaCode area_code){
		Lock l = new Lock(type, 0, TID, id, TableName, area_code);
		addLock(l);
		return ;
	}
	
	private void addLock(Lock L){
		L.WaitforT = getLatestWaitfor(L);
		LockTable.add(L);
		return ;
	}
	
	//use
	public void releaseLock(int TID){
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

	private Set<Integer> cycleDectect(){
		Graph WaitforGraph = new Graph();
		int i = LockTable.size();
		while( i >= 0 ){
			WaitforGraph.addEdge(LockTable.get(i).TID, LockTable.get(i).WaitforT);
			--i;
		} 
		return WaitforGraph.checkCycle();
	}
	
	//use
	public int DeadLockDetectFree(){
		Set<Integer> DeadVertex = cycleDectect();
		int DeadVertexTID = -1;
		if(!DeadVertex.isEmpty()){
			DeadVertexTID = DeadVertex.iterator().next();
			//get dead TID, then clear the table entry whose wait for field is this TID
			releaseLock(DeadVertexTID);
		}
		return DeadVertexTID;
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
		public IDNumber ID;//Record ID
		public String TableName;
		public data.AreaCode AreaCode;
		public int WaitforT;
		
		public Lock(Type type, int Stamp, int TID, IDNumber id2, String TableName, data.AreaCode area_code){
			this.Ttype = type;
			this.TimeStamp = Stamp;
			this.TID = TID;
			this.ID = id2;
			this.TableName = TableName;
			this.AreaCode = area_code;
			this.WaitforT = -1;
		}
		
	}
}
