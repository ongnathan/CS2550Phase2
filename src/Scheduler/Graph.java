package Scheduler;
import java.util.*;

class Graph{
		private HashMap<Integer, HashMap<Integer, Boolean>> disjoint_matrix = new HashMap<Integer, HashMap<Integer, Boolean>>( );
		public Graph(){
			
		}

		public boolean addEdge(int v2, int v1){
			if(disjoint_matrix.get(v1)!=null){
				disjoint_matrix.get(v1).put(v2, true);
			}else{
				disjoint_matrix.put(v1, new HashMap<Integer, Boolean>());
				disjoint_matrix.get(v1).put(v2, true);
			}
			//then add v2
			if(disjoint_matrix.get(v2)!=null){
				// do nothing
			}else{
				disjoint_matrix.put(v2, new HashMap<Integer, Boolean>());
			}
			return true;
		}
		
		/*
		 * Notice: checkCycle will return set of vertexes, 
		 * please use isEmpty to check weather there is cycle, DO NOT use "==null" 
		 * */
		public Set<Integer> checkCycle(){
			int lasttime = 0;
			int thistime = disjoint_matrix.size();
			while(lasttime!=thistime){
				Set<Integer> iterset= disjoint_matrix.keySet();
				for(int iter : iterset){
					if(disjoint_matrix.get(iter).isEmpty()){
						disjoint_matrix.remove(iter);
						Set<Integer> jterset =  disjoint_matrix.keySet();
						for(int jter : jterset){
							if(disjoint_matrix.get(jter).get(iter)!=null)
								disjoint_matrix.get(jter).remove(iter);
						}
						break;
					}
				}
				lasttime = thistime;
				thistime = disjoint_matrix.size();
			}
			return disjoint_matrix.keySet();
		}	
}

