package Scheduler;

import java.awt.Graphics;
import java.util.ArrayList;
import java.util.IllegalFormatCodePointException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import data.AreaCode;
import data.IDNumber;

public class Scheduler {
	ArrayList<Lock> LockTable = new ArrayList<Lock>();
	long timestamp;
	
	public Scheduler() {
		timestamp = 0;
	}
	
	/**
	 *addTupleLock: return value is: if this transaction has to wait for someone or not
	 *for true is yes, otherwise is not
	 */
	public boolean addTupleLock(Type type, int TID, IDNumber id, String TableName, AreaCode area_code){
		Lock l = new Lock(type, 0, TID, id, TableName, area_code);
		return addLock(l);
//			if(!LockTable.contains(l)){
//				LockTable.add(l);
//			}
	}
	
	private boolean addLock(Lock L){
		L.WaitforT = getLatestWaitfor(L);
		LockTable.add(L);
		return !L.WaitforT.isEmpty();
	}
	
	public void releaseLock(int TID){
		int i = LockTable.size()-1;
		while( i >= 0 ){
			if( LockTable.get(i).TID == TID ){
				LockTable.remove(i);
			} else if( LockTable.get(i).WaitforT.contains(TID) )
				LockTable.get(i).WaitforT.remove(TID);
			--i;
		}
		return ;
	}
	
	private LinkedList<Integer> getLatestWaitfor(Lock L){
		int i = LockTable.size()-1;
		LinkedList<Integer> ret = new LinkedList<Integer>();
		while( i >= 0 ){
			if(CompatTable(L.Ttype,LockTable.get(i).Ttype)){
					--i;
					continue;
			}else{
					if( L.TableName.equals(LockTable.get(i).TableName) &&  ( ( (L.ID!=null)  &&   L.ID.equals(LockTable.get(i).ID)  ) || (L.AreaCode!=null) &&  L.AreaCode.equals(LockTable.get(i).AreaCode)) )
					{
						ret.add(LockTable.get(i).TID);
					}
					--i;
			}
		}
		return ret;
	}

	private Set<Integer> cycleDectect(){
		Graph WaitforGraph = new Graph();
		int i = LockTable.size()-1;
		while( i >= 0 ){
			for( int j = 0; j<LockTable.get(i).WaitforT.size();++j){
				WaitforGraph.addEdge(LockTable.get(i).TID, LockTable.get(i).WaitforT.get(j));
			}
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
		public LinkedList<Integer> WaitforT;
		
		public Lock(Type type, int Stamp, int TID, IDNumber id2, String TableName, data.AreaCode area_code){
			this.Ttype = type;
			this.TimeStamp = Stamp;
			this.TID = TID;
			this.ID = id2;
			this.TableName = TableName;
			this.AreaCode = area_code;
			this.WaitforT = null;
		}
		
	}
}
