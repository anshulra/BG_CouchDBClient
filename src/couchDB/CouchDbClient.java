/**
 * 
 * Authors:  Anshul Rawat and Shashank Singh
 *
 * 
 */

package couchDB;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Document;
import com.fourspaces.couchdb.Session;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class CouchDbClient extends DB {

	private boolean manipulationArray;
    private boolean friendListReq;
    private Session session;
    private Database users_db;
    private Database resources_db;
    private static AtomicInteger NumThreads = null;
	private static Semaphore crtcl = new Semaphore(1, true);
	private static Properties props;
	private static String couch_url = "100.112.216.144";
	private static int couch_port = 5984;
    private static String users = "users";
    private static String resources = "resources";
    private static String relative_address = "/BG/pythoncode/";
    
    private static int incrementNumThreads() {
		int v;
		do {
			v = NumThreads.get();
		} while (!NumThreads.compareAndSet(v, v + 1));
		return v + 1;
	}

	private static int decrementNumThreads() {
		int v;
		do {
			v = NumThreads.get();
		} while (!NumThreads.compareAndSet(v, v - 1));
		return v - 1;
	}


	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	public boolean init() throws DBException {
        try {
        	//System.out.println(couch_url);

            try {
                crtcl.acquire();

                if (NumThreads == null) {
                    NumThreads = new AtomicInteger();
                    NumThreads.set(0);
                }
                session = new Session(couch_url,couch_port);
                users_db = session.getDatabase(users);
                resources_db = session.getDatabase(resources);
                incrementNumThreads();
             

            } catch (Exception e) {
                System.out.println("CouchDB init failed to acquire semaphore.");
                e.printStackTrace(System.out);
            } finally {
                crtcl.release();
            }
        } catch (Exception e1) {
            System.out
                    .println("Could not initialize CouchDB connection pool for Loader: "
                            + e1.toString());
            e1.printStackTrace(System.out);
            return false;
        }
        return true;
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key.
	 *
	 * @param entitySet The name of the table
	 * @param entityPK The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
	 */
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage) {
		Database db = null;
		try{
			if(entitySet.equalsIgnoreCase("users"))
				db = users_db;
			else
				db = resources_db;
			Document doc = new Document();
			doc.setId(entityPK);
			//add key value pairs to doc
			for(String k: values.keySet()) {
				if(!(k.toString().equalsIgnoreCase("pic") || k.toString().equalsIgnoreCase("tpic")))
					doc.put(k, values.get(k).toString());
			}
			if(entitySet.equalsIgnoreCase("users")){
				//create friend and wall resource list for users
				doc.put("ConfFriends",new JSONArray());
				doc.put("PendFriends",new JSONArray());
				doc.put("WallResources", new JSONArray());
				//add images
				if(insertImage) {
					byte[] profileImage = ((ObjectByteIterator)values.get("pic")).toArray();
					doc.put("pic",profileImage);
					byte[] thumbImage = ((ObjectByteIterator)values.get("tpic")).toArray();
					doc.put("tpic",thumbImage);
				}
			}
			//add resource to user wall
			if(entitySet.equalsIgnoreCase("resources")){
				String walluserid = (String)doc.get("walluserid");
				Document doc2 = users_db.getDocument(walluserid);
				JSONArray wallresources = doc2.getJSONArray("WallResources");
				wallresources.add(0, doc.getId());
				
				doc2.put("WallResources", wallresources);
				users_db.saveDocument(doc2);
			}
			db.saveDocument(doc);
			
		}catch(Exception e){
			e.printStackTrace();
			System.out.println(e.toString());
			return -1;
		}
		return 0 ;
	}



	public int viewProfile(int requesterID, int profileOwner,HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {
		int retval = 0;
		if( requesterID < 0 || profileOwner < 0)
			return -1;
		try{
			Document doc = users_db.getDocument(profileOwner+"");
		//check if image is present
		try{
			byte[] img = (byte[])doc.get("pic");
			result.put("pic", new ObjectByteIterator(img));
			if(testMode){
				//save image as a new file
				FileOutputStream fos = new FileOutputStream(profileOwner+"--proimage.bmp");
				fos.write(img);
				fos.close();
			}
			doc.remove("pic");
			doc.remove("tpic");
		}catch(Exception e){}
		//number of pending friends and number of confirmed friends
		int frndCount = doc.getJSONArray("ConfFriends").size();
		int pendCount = 0;
		if(requesterID == profileOwner)
			pendCount = doc.getJSONArray("PendFriends").size();
		int resCount = doc.getJSONArray("WallResources").size();
		doc.remove("ConfFriends");
		doc.remove("PendFriends");
		doc.remove("WallResources");
		Set<String> keys = (Set<String>)doc.keySet();
		//remove confFriends and pendFriends from keyset
		Iterator it = keys.iterator();
		while(it.hasNext()){
		String key = (String)it.next();
		result.put(key, new ObjectByteIterator(doc.getString(key).getBytes()));
		}
		result.put("friendcount", new ObjectByteIterator(Integer.toString(frndCount).getBytes()));
		result.put("pendingcount", new ObjectByteIterator(Integer.toString(pendCount).getBytes()));
		result.put("resourcecount", new ObjectByteIterator(Integer.toString(resCount).getBytes()));
		}catch (Exception e){
			System.out.println("View Profile error for user "+requesterID+" for profile "+profileOwner);
			System.out.println(e.toString());;
			retval = -1;
		}
		return retval;
	}

	public int listFriends(int requesterID, int profileOwnerID,
						   Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
		int retval = 0;
		if (requesterID < 0 || profileOwnerID < 0)
			return -1;
		//get all confirmed friends for profileOwnerID
		try{
		Document doc = users_db.getDocument(profileOwnerID+"");
		JSONArray conflist = doc.getJSONArray("ConfFriends");
		for(int i = 0;i< conflist.size();i++) {
			int friendid = conflist.getInt(i);
			Document friend = users_db.getDocument(friendid+"");
			HashMap<String,ByteIterator> vals = new HashMap<String,ByteIterator>();
			if(insertImage){
				byte[] img = (byte[])friend.get("tpic");
				vals.put("tpic", new ObjectByteIterator(img));
				if(testMode){
					//save image as a new file
					FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"
							+(i+1)+"-mthumbimage.bmp");
					fos.write(img);
					fos.close();
				}
				friend.remove("pic");
				friend.remove("tpic");
			}
			//remove confFriends,pendFriends,WallRes from keyset
			
			friend.remove("ConfFriends");
			friend.remove("PendFriends");
			friend.remove("WallResources");
			Set<String> keys = (Set<String>)friend.keySet();
			Iterator it = keys.iterator();
			while(it.hasNext()){
			String key = (String)it.next();
			vals.put(key, new ObjectByteIterator(friend.getString(key).getBytes()));
			}
			result.add(vals);
		}
		}catch(Exception e){
			System.out.println(e.toString());
			System.out.println("Error for user"+requesterID+" and profileowner"+profileOwnerID);
			retval = -1;
		}
		return retval;

	}

	public int viewFriendReq(int profileOwnerID,
							 Vector<HashMap<String, ByteIterator>> values, boolean insertImage, boolean testMode) {
		int retval = 0;
		if (profileOwnerID < 0)
			return -1;
		//get all confirmed friends for profileOwnerID
		try{
		Document doc = users_db.getDocument(profileOwnerID+"");
		JSONArray pendlist = doc.getJSONArray("PendFriends");
		for(int i = 0;i< pendlist.size();i++) {
			int friendid = pendlist.getInt(i);
			Document friend = users_db.getDocument(friendid+"");
			HashMap<String,ByteIterator> vals = new HashMap<String,ByteIterator>();
			if(insertImage){
				byte[] img = (byte[])friend.get("tpic");
				vals.put("tpic", new ObjectByteIterator(img));
				if(testMode){
					//save image as a new file
					FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"
							+(i+1)+"-mthumbimage.bmp");
					fos.write(img);
					fos.close();
				}
				friend.remove("pic");
				friend.remove("tpic");
			}
			//remove confFriends and pendFriends from friendprofile 
			friend.remove("ConfFriends");
			friend.remove("PendFriends");
			friend.remove("WallResources");
			
			Set<String> keys = (Set<String>)friend.keySet();
			Iterator it = keys.iterator();
			while(it.hasNext()){
			String key = (String)it.next();
			vals.put(key, new ObjectByteIterator(friend.getString(key).getBytes()));
			}
			values.add(vals);
		}
		}catch(Exception e){
			System.out.println(e.toString());
			retval = -1;
		}
		return retval;


	}

	public int acceptFriend(int invitorID, int inviteeID) {
		//delete from pending of the invitee
		//add to confirmed of both invitee and invitor
		try{
			Document invitee = users_db.getDocument(inviteeID+"");
			Document invitor = users_db.getDocument(invitorID+"");
			//add to confirmed of invitor
			JSONArray invitor_confirmed = invitor.getJSONArray("ConfFriends");
			invitor_confirmed.add(inviteeID);
			invitor.put("ConfFriends",invitor_confirmed);
			//add to confirmed of invitee
			JSONArray invitee_confirmed = invitee.getJSONArray("ConfFriends");
			invitee_confirmed.add(invitorID);
			invitee.put("ConfFriends",invitee_confirmed);
			//remove from pending of invitee
			JSONArray invitee_pending = invitee.getJSONArray("PendFriends");
			for(int i=0;i<invitee_pending.size();i++) {
				if(invitee_pending.getInt(i)==invitorID) {
					invitee_pending.remove(i);
					break;
					}
			}
			invitee.put("PendFriends", invitee_pending);
			users_db.saveDocument(invitor);
			users_db.saveDocument(invitee);
		}catch(Exception e){
			System.out.println(e.toString());
			return -1;
		}
		return 0;

	}

	public int rejectFriend(int invitorID, int inviteeID) {
		int retVal = 0;
		if(invitorID < 0 || inviteeID < 0)
			return -1;
		try{
			Document invitee = users_db.getDocument(inviteeID+"");
			JSONArray pending = invitee.getJSONArray("PendFriends");
			for(int i=0;i<pending.size();i++){
				if(pending.getInt(i)==invitorID) {
					pending.remove(i);
					break;
				}
			}
			invitee.put("PendFriends", pending);
			users_db.saveDocument(invitee);
		}catch(Exception e){
			System.out.println(e.toString());
			return -1;
		}
		return retVal;
	}

	public int inviteFriend(int invitorID, int inviteeID) {
		try{
			Document invitee = users_db.getDocument(inviteeID+"");
			JSONArray pending = invitee.getJSONArray("PendFriends");
			pending.add(invitorID);
			invitee.put("PendFriends", pending);
			users_db.saveDocument(invitee);
		}catch(Exception e){
			System.out.println(e.toString());
			return -1;
		}
		return 0;
	}

	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
								 Vector<HashMap<String, ByteIterator>> result) {
		int retVal = 0;
		if (profileOwnerID < 0 || requesterID < 0 || k < 0)
			return -1;
		try{
			Document doc = users_db.getDocument(profileOwnerID+"");
			JSONArray wallres = doc.getJSONArray("WallResources");
			int i = 0;
			while(i <= k && i < wallres.size()){
				HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
				Document doc1 = resources_db.getDocument(wallres.get(i)+"");
				String tmp = doc1.get("_id")+"";
				vals.put("rid", new ObjectByteIterator(tmp.getBytes()));
				tmp = doc1.get("walluserid")+"";
				vals.put("walluseridid", new ObjectByteIterator(tmp.getBytes()));
				tmp = doc1.get("creatorid")+"";
				vals.put("creatorid", new ObjectByteIterator(tmp.getBytes()));
				i++;
				result.add(vals);
			}
		}catch(Exception e){
				System.out.println(e.toString());
				retVal = -1;
		}
		return retVal;
	}


	public int getCreatedResources(int creatorID,
								   Vector<HashMap<String, ByteIterator>> result) {
		int retVal = 0;
		if(creatorID < 0)
			return -1;
		String key = Integer.toString(creatorID);
		try{
			ProcessBuilder pb = new ProcessBuilder("python", relative_address + "getcreatedres.py", key);
			Process p = pb.start();
			String line = null;
	        BufferedReader br1 = new BufferedReader(new InputStreamReader(p.getInputStream()));
	        while((line=br1.readLine()) != null) {
	        	HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
	        	String a[] = new String[2];
	        	a = line.split(",", 2);
	        	String str = a[0].replaceAll("\\D+","");
	        	String str1 = a[1].replaceAll("\\D+","");
	        	vals.put("rid",new ObjectByteIterator(str.getBytes()));
	        	vals.put("creatorid",new ObjectByteIterator(str1.getBytes()));
	        	result.add(vals);
	        }
		}catch(Exception e){
			System.out.println(e.toString());
			retVal = -1;
		}
		//System.out.println(result);
		return retVal;
	}


	public int viewCommentOnResource(int requesterID, int profileOwnerID,
									 int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		if (profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
			return -1;
		try{
		Document doc = resources_db.getDocument(resourceID+"");
			try{	
			JSONArray allcomments = doc.getJSONArray("manipulations");
			result.addAll(allcomments);
			}catch(Exception e){}
			
		}catch(Exception e){
			System.out.println(e.toString());
			return -1;
		}
		return 0;
		
	}
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
									 int resourceID, HashMap<String,ByteIterator> commentValues) {
		int retVal = 0;
		if(profileOwnerID < 0 || commentCreatorID < 0 || resourceID < 0)
			return -1;
		try{
		Document doc = resources_db.getDocument(resourceID+"");
		// how to check whether manipulations exist or not
		JSONArray result;
		try{
			result = doc.getJSONArray("manipulations");
		}catch(Exception e){
			result = new JSONArray();
		}
		HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
		values.put("mid", commentValues.get("mid"));
		values.put("creatorid",new ObjectByteIterator(Integer.toString(profileOwnerID).getBytes()));
		values.put("rid", new ObjectByteIterator(Integer.toString(resourceID).getBytes()));
		values.put("modifierid", new ObjectByteIterator(Integer.toString(commentCreatorID).getBytes()));
		values.put("timestamp",commentValues.get("timestamp"));
		values.put("type", commentValues.get("type"));
		values.put("content", commentValues.get("content"));
		result.add(values.toString());
		doc.put("manipulations", result);
		resources_db.saveDocument(doc);
		}catch(Exception e){
			System.out.println(e.toString());
			retVal = -1;
		}
		return retVal;
	}

	public int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID) {
		int retVal = 0;
		try{
		Document doc = resources_db.getDocument(resourceID+"");
		JSONArray allcomments = doc.getJSONArray("manipulations");
		for(int i=0;i<allcomments.size();i++){
			JSONObject values = (JSONObject) allcomments.get(i);
			if(values.get("mid").equals(manipulationID)){
				allcomments.remove(i);
				break;
			}
		}
		doc.put("manipulations", allcomments);
		resources_db.saveDocument(doc); } catch(Exception e){
			System.out.println(e.toString());
			retVal = -1;
		}
		return retVal;
		
	}

	public int thawFriendship(int friendid1, int friendid2) {
		int retVal = 0;
		try{
		Document doc1 = users_db.getDocument(friendid1+"");
		Document doc2 = users_db.getDocument(friendid2+"");
		JSONArray confFriends1 = doc1.getJSONArray("ConfFriends");
		JSONArray confFriends2 = doc2.getJSONArray("ConfFriends");
		for (int i = 0; i < confFriends1.length(); i++) {
			  if(confFriends1.get(i).equals(friendid2))
				  confFriends1.remove(i);
		}
		for (int i = 0; i < confFriends2.length(); i++) {
			  if(confFriends2.get(i).equals(friendid1))
				  confFriends2.remove(i);
		}
		doc1.put("ConfFriends", confFriends1);
		doc2.put("ConfFriends", confFriends2);
		users_db.saveDocument(doc1);
		users_db.saveDocument(doc2);
		}catch(Exception e){
			System.out.println(e.toString());
			retVal = -1;
		}
		return retVal;
	}

	public HashMap<String, String> getInitialStats() {

		HashMap<String, String> stats = new HashMap<String, String>();
		Process p1 = null;
		Process p2 = null;
		Process p3 = null;
		Process p4 = null;
		BufferedReader br1 = null;
		BufferedReader br2 = null;
		BufferedReader br3 = null;
		BufferedReader br4 = null;
		int usercnt = 0;
		int avgfriends = 0;
		int avgresources = 0;
		int avgpendfrnds = 0;
		try {
			p1 = new ProcessBuilder("python", relative_address + "countusersDesDoc.py").start();
			p2 = new ProcessBuilder("python", relative_address + "countfriendsDesDoc.py").start();
			p3 = new ProcessBuilder("python", relative_address + "countPfriendsDesDoc.py").start();
			p4 = new ProcessBuilder("python", relative_address + "countresourcesDesDoc.py").start();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		br1 = new BufferedReader(new InputStreamReader(p1.getInputStream()));
		br2 = new BufferedReader(new InputStreamReader(p2.getInputStream()));
		br3 = new BufferedReader(new InputStreamReader(p3.getInputStream()));
		br4 = new BufferedReader(new InputStreamReader(p4.getInputStream()));
        try {
			usercnt = Integer.parseInt(br1.readLine());
			avgfriends = Integer.parseInt(br2.readLine())/usercnt;
			avgpendfrnds = Integer.parseInt(br3.readLine())/usercnt;
			avgresources = Integer.parseInt(br4.readLine())/usercnt;
		} catch (IOException e) {
			e.printStackTrace();
		}
        stats.put("usercount", usercnt+"");
  		stats.put("avgfriendsperuser", avgfriends+"");
  		stats.put("avgpendingperuser", avgpendfrnds+"");
  		stats.put("resourcesperuser", avgresources+"");
		return stats;
		
	}

	public void cleanup(boolean warmup) throws DBException
	{
		if(!warmup){
			try {
				crtcl.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			decrementNumThreads();
			// add instance to vector of connections
			if (NumThreads.get() > 0) {
				crtcl.release();
				return;
			} else {
				// close all connections in vector
				try {
					Thread.sleep(6000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				crtcl.release();
			}
		}
	}

	public int CreateFriendship(int memberA, int memberB) {
		try{
			Document user1 = users_db.getDocument(memberA+"");
			Document user2 = users_db.getDocument(memberB+"");
			JSONArray user1_friends = user1.getJSONArray("ConfFriends");
			JSONArray user2_friends = user2.getJSONArray("ConfFriends");
			user1_friends.add(memberB);
			user2_friends.add(memberA);
			user1.put("ConfFriends", user1_friends);
			user2.put("ConfFriends",user2_friends);
			users_db.saveDocument(user1);
			users_db.saveDocument(user2);
		}catch(Exception e){
			System.out.println(e.toString());
			return -1;
		}
		
		return 0;
	}

	public int queryPendingFriendshipIds(int profileId,
										 Vector<Integer> pendingFrnds) {
		int retVal = 0;
		try{
			Document doc = users_db.getDocument(profileId+"");
			JSONArray pending = doc.getJSONArray("PendFriends");
			for(int i=0;i<pending.size();i++){
				pendingFrnds.addElement(pending.getInt(i));
			}
		}catch(Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		}
		
		return retVal;
	}

	public int queryConfirmedFriendshipIds(int profileId,Vector<Integer> confFrnds) {
		int retVal = 0;
		try{
			Document doc = users_db.getDocument(profileId+"");
			JSONArray confirmed = doc.getJSONArray("ConfFriends");
			for(int i=0;i<confirmed.size();i++){
				confFrnds.addElement(confirmed.getInt(i));
			}
		}catch(Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		}
		
		return retVal;
	}

	public void createSchema(Properties props){
        Session s = null;
		// drop all collections
		try {
			// drop database
			session.deleteDatabase(users);
			session.deleteDatabase(resources);
			//create database
			session.createDatabase(users);
			session.createDatabase(resources);


		} catch (Exception e) {
			System.out.println(e.toString());
			return;
		}

	}

}
