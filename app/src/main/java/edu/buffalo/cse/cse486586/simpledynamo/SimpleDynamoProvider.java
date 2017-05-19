package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final int SERVER_PORT = 10000;
	private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	private ContentResolver mContentResolver;
	private ContentValues mContentValues ;
	ServerSocket serverSocket;
	TreeMap<String, String> map = new TreeMap<String, String>();
	String portStr;
	Object insertobj = new Object();
	final Lock lock = new ReentrantLock();
	final Condition recoverobj= lock.newCondition();
	final Condition queryobj = lock.newCondition();
	ArrayList<String> list = new ArrayList<String>();

	String[] col = {"key","value"};
	MatrixCursor cursor = new MatrixCursor(col);

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

			if(selection.equals("@")){
				Log.d(TAG, "delete all local elements");
				deletelocal();
			} else{
				if(selection.equals("*")){
					Log.d(TAG, "delete from all nodes");
					deleteall();
				}
				else{
					Log.d(TAG, "deleting elements");
					deletefile(selection);
				}
			}

		return 0;
	}

	public void deletefile(String key){
		/*If a key is deleted then delete all of its replicas as well on successor nodes.*/
		Iterator<Map.Entry<String, String>> itr = map.entrySet().iterator();
		try{
			while(itr.hasNext()){
				Map.Entry ob = itr.next();
				if(portStr.equals(ob.getValue().toString())){
					for(int i=0;i<3;i++){
						String port = String.valueOf(Integer.parseInt(ob.getValue().toString())*2);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(port));

						if(!itr.hasNext()){
							itr = map.entrySet().iterator();
						}
						ob = itr.next();

						if(socket.isConnected()){
							PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
							writer.println("deletefile:"+key);
							writer.flush();
						}
						else{
							continue;
						}
					}
					break;
				}
			}
		}catch(Exception e){
			Log.e(TAG, "Exception in deletefile-->"+e);
		}
	}

	public void deletelocal(){
		File dir = new File(getContext().getFilesDir().getPath());
		File[] files = dir.listFiles();
		for(File file:files){
			if(file.exists()){
				getContext().deleteFile(file.toString().replaceAll(getContext().getFilesDir().getPath() + "/", ""));
			}
		}
		Log.d(TAG, "all local files deleted");
	}
	public void deleteall(){
		Iterator<Map.Entry<String, String>> itr = map.entrySet().iterator();
		try{
			while(itr.hasNext()){
				Map.Entry ob = itr.next();
				String senderport = String.valueOf(Integer.parseInt(ob.getValue().toString())*2);
				Socket socket= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(senderport));
				if(socket.isConnected()){
					PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
					writer.println("delete");
					writer.flush();
				}else{
					Log.e(TAG, "socket not connected during deleteall-->"+ob.getValue().toString());
					continue;
				}
			}
			Log.d(TAG, "All elements deleted from all avds");
		}catch(Exception e){
			Log.e(TAG,"Exception in readall-->"+e);
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key = values.get("key").toString();
		String value = values.get("value").toString();

		try {
			String keyhash = genHash(key);
			/*
			* 1. Iterate through the map where node's hash values are stored in increasing order.
			* 2. If the hash value of a node>=keyhash then send the insert request to the node and it's two successors
			* 3. During insertion add the port number at the end of value string. During querying, parse the value string and
			* remove the port number before returning key values pair.
			* */
			Iterator<Map.Entry<String, String>> itr = map.entrySet().iterator();

				while (itr.hasNext()) {
					Map.Entry ob = itr.next();
					if (ob.getKey().toString().compareTo(keyhash) >= 0 || keyhash.compareTo(map.lastKey()) > 0) {
						String port = ob.getValue().toString();
						Log.d(TAG, "insert port found-->" + port + ":" + key);
						value = value + "@port" + port;
						for (int i = 0; i < 3; i++) {
							String senderport = String.valueOf(Integer.parseInt(port) * 2);
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(senderport));
							if (socket.isConnected()) {
								PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
								writer.println("insert:" + key + ":" + value);
								writer.flush();
							} else {
								Log.d(TAG, "socket not connected during insert");
								if (!itr.hasNext()) {
									itr = map.entrySet().iterator();
								}
								port = itr.next().getValue();
								continue;
							}
							if (!itr.hasNext()) {
								itr = map.entrySet().iterator();
							}
							port = itr.next().getValue();
						}
						break;
					}
				}
		}
		catch(Exception e){
			Log.e(TAG, "Exception insert-->"+portStr+":"+e);
		}
		return null;
	}

 	public synchronized void insertrecord(String key, String val){
		/*synchronized since there should not be any conflict during concurrent insertions*/
			try {
				OutputStream os = getContext().openFileOutput(key, getContext().MODE_PRIVATE);
				os.write(val.getBytes());
				Log.d(TAG, "insert successful-->" + key + ":" + val);
			} catch (Exception e) {
				Log.e(TAG, "Exception in insertion-->" + e);
			}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		try{
			/*
			* 1. Crete a static treemap of 5 nodes which will store the nodes in increasing hash value order
			* 2. Initialize a serversocket
			* 3. During creation of a node stores a key "isrecover".
			* 4. During recovery check for the file "isrecovery" and make a call to client thread to recover all the values
			* 5. wait until all the objects are recovered.
			* */
			for(int i=5554;i<=5562;i+=2){
				String key = genHash(String.valueOf(i));
				map.put(key, String.valueOf(i));
			}

			serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			Log.d(TAG, "entered server socket");

			lock.lock();
			try {
				InputStreamReader fin = new InputStreamReader(getContext().openFileInput("isrecover"));
				BufferedReader reader = new BufferedReader(fin);
				if (reader != null) {
					Log.d(TAG, "send request for recovery");
					new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "recover", portStr);
					recoverobj.await();
				}
				} catch (Exception FileNotFoundException) {
					Log.d(TAG, "inserting recover file");
					insertrecord("isrecover", " ");
				}finally{
					lock.unlock();
				}

		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket: "+e);
			return false;
		}
		catch(Exception e){
			Log.e(TAG, "error in oncreate: "+e);
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		MatrixCursor cur = new MatrixCursor(col);

		if(selection.equals("@")){
			if(readlocal()!=""){
				String[] results = readlocal().split("%");
				for(String result:results){
					String[] r = result.split("-->");
					cur.addRow(r);
				}
			}

		}else{
			if(selection.equals("*")){
				String[] results = readall().split("%");
				for(String result:results){
					String[] r = result.split("-->");
					cur.addRow(r);
				}

			}else{
				String[] result = read(selection).split(":");
				cur.addRow(result);
			}
		}

		if(cur!=null){
			cur.moveToFirst();
			do {
				Log.d(TAG, "query: "+selection+cur.getCount()+":"+cur.getString(0)+":"+cur.getString(1));
			}
			while(cur.moveToNext());
		}
		return cur;
	}

	public String readlocal(){
		File dir = new File(getContext().getFilesDir().getPath());
		File[] files= dir.listFiles();
		String result = "";
		if(files.length==0){
			return "";
		}
		try{
			for(File file:files){
				if(file.isFile()){
					String filename = file.toString().replaceAll(getContext().getFilesDir().getPath()+"/", "");
					if(filename.equals("isrecover")){
						continue;
					}
					InputStream fin = new FileInputStream(new File(file.toString()));
					BufferedReader br = new BufferedReader(new InputStreamReader(fin));
					String[] val = br.readLine().split("@port");
					result += filename+"-->"+val[0]+"%";
				}
			}

		}catch(Exception e){
			Log.e(TAG, "Exception in readlocal-->"+e);
		}
		return result;
	}

	public String readall(){
		Iterator<Map.Entry<String, String>> itr = map.entrySet().iterator();
		String result = "";
		String temp = "";
		try{
			while(itr.hasNext()){
				Map.Entry ob = itr.next();
				String senderport = String.valueOf(Integer.parseInt(ob.getValue().toString())*2);
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(senderport));
				if(socket.isConnected()){
					PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
					writer.println("queryall");
					writer.flush();
				}else{
					continue;
				}
				if(socket.getInputStream()==null){
					continue;
				}
				BufferedReader reader= new BufferedReader(new InputStreamReader(socket.getInputStream()));
				temp = reader.readLine();
				if(temp!=null){
					result += temp;
				}
			}
		}catch(Exception e){
			Log.e(TAG, "Exception in readll-->"+e);
		}
		Log.d(TAG, "* query result-->"+result);
		return result;
	}

	public String read(String key){
		Iterator<Map.Entry<String, String>> itr = map.entrySet().iterator();
		String prev = null;
		String result = "";
		String temp = "";
		try{
			while(itr.hasNext()){
				Map.Entry ob = itr.next();

				if(haskey(key, prev, ob.getValue().toString())){
					Log.d(TAG, "query port found-->"+ob.getValue()+":"+key);
					for(int i=0;i<3;i++){
						String senderport = String.valueOf(Integer.parseInt(ob.getValue().toString())*2);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(senderport));
						Log.d(TAG, "send query to-->"+senderport);

						if(!itr.hasNext()){
							itr = map.entrySet().iterator();
						}
						ob = itr.next();

						if(socket.isConnected()){
							PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
							writer.println("query:"+key);
							writer.flush();
						}else{
							continue;
						}
						if(socket.getInputStream()==null){
							continue;
						}
						BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						temp = reader.readLine();
						Log.d(TAG, "query response recieved-->"+ob.getValue().toString()+":"+temp);
						if(temp!=null){
							result= temp;
						}
					}
					break;
				}
				prev = ob.getKey().toString();
			}
		}catch(Exception e){
			Log.e(TAG, "Exception in read query-->"+e);
		}
		Log.d(TAG, "query result-->"+result);
		return result;
	}

	public boolean haskey(String key, String prevnode, String curnode){

		try{
			String keyhash = genHash(key);
			String curnodehash = genHash(curnode);
			if(prevnode==null){
				if(keyhash.compareTo(map.lastKey())>0 || keyhash.compareTo(map.firstKey())<0){
					Log.d(TAG, curnode+" has key");
					return true;
				}
			}
			else{
				if(keyhash.compareTo(curnodehash)<=0&&keyhash.compareTo(prevnode)>0){
					Log.d(TAG, curnode+" has key");
					return true;
				}
			}
		}catch(Exception e){
			Log.e(TAG, "Exception in haskey-->"+e);
		}
		return false;
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					Socket socket = serverSocket.accept();
					Log.d(TAG, sockets[0].toString());
					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String message[] = br.readLine().split(":");

					if(message[0].equals("insert")){
						String key= message[1];
						String val = message[2];
						lock.lock();

						try{
							insertrecord(key, val);
						}catch(Exception e){
							Log.e(TAG, "Exception in insert at server-->"+e);
						}
						finally{
							lock.unlock();
						}

					}

					if(message[0].equals("query")){

						String key = message[1];

						try{
							InputStreamReader fin = new InputStreamReader(getContext().openFileInput(key));
							BufferedReader reader = new BufferedReader(fin);
							String[] result = reader.readLine().split("@port");
							PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
							writer.println(key+":"+result[0]);
							writer.flush();
						}catch(Exception e){
							Log.d(TAG, "Exception in query at server-->"+e);
						}
					}

					if(message[0].equals("queryall")){
						PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
						writer.println(readlocal());
						writer.flush();
					}

					if(message[0].equals("delete")){
						deletelocal();
					}

					if(message[0].equals("deletefile")){
						String key = message[1].trim();
						File file = getContext().getFileStreamPath(key);
						if(file.exists()){
							Log.d("TAG", "file deleted");
							getContext().deleteFile(key);
						}
					}

					if(message[0].equals("recover")) {

						String originalport = message[1];
						String prev = message[2];
						String succ = message[3];
						String prevpred = "";
						String result = "";
						if (portStr.equals(prev)) {
							Iterator<Map.Entry<String, String>> itr = map.entrySet().iterator();
							while (itr.hasNext()) {
								Map.Entry obj = itr.next();
								if (portStr.equals(obj.getValue().toString())) {
									break;
								}
								prevpred = obj.getValue().toString();
							}
							if (prevpred.equals("")) {
								prevpred = map.get(map.lastKey());
							}
						}

						File dir = new File(getContext().getFilesDir().getPath());
						File[] files = dir.listFiles();
						if (files.length == 0) {
							result = "";
						} else {
						try {
							for (File f : files) {
								if (f.isFile()) {
									String filename = f.toString().replaceAll(getContext().getFilesDir().getPath() + "/", "");
									if (filename.equals("isrecover")) {
										continue;
									}

									InputStream fin = new FileInputStream(new File(f.toString()));
									BufferedReader reader = new BufferedReader(new InputStreamReader(fin));
									String[] val = reader.readLine().split("@port");
									if (portStr.equals(prev)) {
										if (val[1].equals(prev) || val[1].equals(prevpred)) {
											result += filename + "-->" + val[0] + "@port" + val[1] + "%";
										}
									} else {
										if (val[1].equals(originalport)) {
											result += filename + "-->" + val[0] + "@port" + val[1] + "%";
										}
									}
								}
							}

						} catch (Exception e) {
							Log.e(TAG, "exception in recovery: " + e);
						}
					}
						PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
						Log.d(TAG, "sending recovery results to:" + originalport + ":" + result);
						writer.println(result);
						writer.flush();
					}
					socket.close();
				}
			}catch(Exception e){
				Log.e(TAG, "Exception in server->"+e);
			}
			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			String message[] = msgs[0].split(":");
			String myport = msgs[1];

			/*Recover the all the objects belong to failed node by sending recovery request to pred and succ*/
			if(message[0].equals("recover")){
				Iterator<Map.Entry<String, String>> itr = map.entrySet().iterator();
				String prev = "";
				String succ = "";
				lock.lock();
					try {
						while (itr.hasNext()) {
							Map.Entry ob = itr.next();
							if (portStr.equals(ob.getValue().toString())) {
								if (!itr.hasNext()) {
									itr = map.entrySet().iterator();
								}
								succ = itr.next().getValue().toString();
								break;
							}
							prev = ob.getValue().toString();
						}
						if (prev.equals("")) {
							prev = map.get(map.lastKey());
						}

						String[] ports = {prev, succ};
						String results = "";

						for (int i = 0; i < ports.length; i++) {
							String senderport = String.valueOf(Integer.parseInt(ports[i]) * 2);
							Log.d(TAG, "sending request for recovery to port-->" + senderport);
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(senderport));
							if (socket.isConnected()) {
								PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
								writer.println("recover:" + portStr + ":" + prev + ":" + succ);
								writer.flush();
							} else {
								throw new Exception("sockets not connected during recovery");
							}
							BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							results += reader.readLine();
						}

						if(!results.equals("")){
							String[] result = results.split("%");

							for (int i=0;i<result.length;i++) {
								String[] s = result[i].split("-->");
								Log.d(TAG, "recovering values-->" + s[0] + ":" + s[1]);
								insertrecord(s[0], s[1]);
							}
						}
						recoverobj.signal();

					} catch (Exception e) {
						Log.e(TAG, "Exception in recovery-->" + e);
					}finally{
						lock.unlock();
					}
			}
			return null;
		}
	}
}
