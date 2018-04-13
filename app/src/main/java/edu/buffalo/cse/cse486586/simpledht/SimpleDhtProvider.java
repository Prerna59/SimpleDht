package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.MatrixCursor;
import android.database.Cursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.security.NoSuchAlgorithmException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.Formatter;
import java.util.Map.Entry;
import java.util.HashMap;
import android.os.AsyncTask;
import java.util.Properties;
import java.io.StringReader;


public class SimpleDhtProvider extends ContentProvider {
    private static final String TAG = SimpleDhtProvider.class.getSimpleName();

    private static final int SERVER_PORT = 10000;
    private static final int START_PORT = 11108;

    private Uri providerUri=Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");

    private int thisPort;
    private String nodeIdHash;
    private String predIdHash;
    private int predPort;
    private String succIdHash;
    private int succPort;

    //creating global messageData object
    private MessageData messageData = new MessageData();


    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager)this.getContext().getSystemService(
                Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        thisPort = Integer.parseInt(portStr) * 2;

        try {
            nodeIdHash = genHash(portStr);
            predIdHash = nodeIdHash;
            succIdHash = nodeIdHash;
            predPort = thisPort;
            succPort = thisPort;

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            if (thisPort != START_PORT) {

              //  Log.v("OnCreate",String.valueOf(START_PORT));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(START_PORT),
                        nodeIdHash,null,String.valueOf(thisPort),String.valueOf(thisPort),"join",null);
            }
        }
        catch (NoSuchAlgorithmException nsae) {
            Log.e(TAG, "NoSuchAlgorithmException:" + nsae.getMessage());
            return false;
        }
        catch (IOException ie) {
            Log.e(TAG, "Can't create a ServerSocket:" + ie.getMessage());
            return false;
        }
        catch (Exception e) {
            Log.e(TAG, "Can't create a ServerSocket:" + e.getMessage());
            return false;
        }

        return true;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.i("Delete method","Deleting process start");
        deleteData(selection, thisPort);
        return 0;
    }

    private void deleteData(String key, int originPort) {
        //*--> Global dump, @-->Local dump
        if (key.equals("@") || key.equals("*")) {
            String[] keyFiles = getContext().fileList();
            for(String file:keyFiles) {
                try {
                    getContext().deleteFile(file);
                }
                catch(Exception e){
                    Log.e(TAG, "Delete operation failed:" + e.getMessage());
                }
            }
            if (key.equals("*") && succPort != originPort) {

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(succPort),
                        key,null,String.valueOf(originPort),String.valueOf(0),"delete",null);
            }
        }
        else {
            try {
                String keyHash = genHash(key);
                if (checkRing(keyHash)) {
                    try {
                        getContext().deleteFile(key);
                    }
                    catch(Exception e){
                        Log.e(TAG, "Delete operation failed:" + e.getMessage());
                    }

                } else {

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(succPort),
                            key,null,String.valueOf(originPort),String.valueOf(0),"delete",null);

                }
            }
            catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "NoSuchAlgorithmException:" + e.getMessage());
            }
        }
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        insertData(values.getAsString("key"), values.getAsString("value"));
        return uri;
    }


    private void insertData(String key, String value){
        try {
         //   Log.v("Key Insert","In first insert method");
            String keyHash = genHash(key);
            FileOutputStream outputStream;
            if(checkRing(keyHash)){
                try {
                    outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                }
                catch (Exception e) {
                    Log.e(TAG, "File write failed");
                }
            }
            else{
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(succPort),
                        key,value,String.valueOf(0),String.valueOf(0),"insert",null);
            }
        }
        catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException:" + e.getMessage());
        }
    }



    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Log.v("Cursor Query","In cursor query method");
        if (selection.equals("@") || selection.equals("*") ) {
            return queryAll(selection, new HashMap<String, String>(), thisPort, 1);
        } else {
       //     Log.v("Cursor Query","key other than * anf @");
       //     Log.v("Cursor Query",selection);
            return queryByKey(selection, thisPort, 1);
        }
    }

    private Cursor queryByKey(String key, int originPort, int wait) {
   //     Log.v("Query Method","Query method started");
        String[] column = {"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(column);
        StringBuilder sb;
        InputStream is;
        try {
            String hash = genHash(key);
            if (checkRing(hash)) {
                if (thisPort == originPort) {
                    sb = new StringBuilder();
                    try {
                        is = getContext().openFileInput(key);
                        BufferedReader br = new BufferedReader(new InputStreamReader(is));
                        String temp;
                        while ((temp = br.readLine()) != null) {
                            sb.append(temp);
                        }
                        br.close();
                    }
                    catch (FileNotFoundException e) {
                        //  e.printStackTrace();
                        Log.e(TAG, " QueryTask UnknownHostException");
                    }
                    catch(IOException ie){
                        // ie.printStackTrace();
                        Log.e(TAG, "QueryTask IOException");
                    }
                    catch(Exception e){
                        //   e.printStackTrace();//general exception
                        Log.e(TAG, "QueryTask General Exception");
                    }

              //      Log.v("Query Method",key);
              //      Log.v("Query Method",sb.toString());
                    String[] row = {key, sb.toString()};
                    matrixCursor.addRow(row);
               //     Log.v("Query Method","After successfully retrieving data");
                    return matrixCursor;

                } else {
                //    Log.v("Cursor Query","In else block");
                    sb = new StringBuilder();
                    try {
                        is = getContext().openFileInput(key);
                        BufferedReader br = new BufferedReader(new InputStreamReader(is));
                        String temp;
                        while ((temp = br.readLine()) != null) {
                            sb.append(temp);
                        }
                        br.close();
                    }
                    catch (FileNotFoundException e) {
                        //  e.printStackTrace();
                        Log.e(TAG, " QueryTask UnknownHostException");
                    }
                    catch(IOException ie){
                        // ie.printStackTrace();
                        Log.e(TAG, "QueryTask IOException");
                    }
                    catch(Exception e){
                        //   e.printStackTrace();//general exception
                        Log.e(TAG, "QueryTask General Exception");
                    }

               //     Log.v("Cursor Query",sb.toString());

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(originPort),
                            null,sb.toString(),String.valueOf(originPort),String.valueOf(0),"queryByKey",null);

                }

            } else {

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(succPort),
                        key,null,String.valueOf(originPort),String.valueOf(0),"queryByKey",null);

                if (wait==1) {
                    try {
                       // Thread.sleep(100);
                        Thread.sleep(200);
                    }
                    catch (InterruptedException e) {
                        Log.e(TAG,"Exception in query sleep()");
                        e.printStackTrace();
                    }
                    matrixCursor.addRow(new String[]{key, messageData.getValue()});
                    messageData.setValue(null);
                }
            }
        }
        catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException:" + e.getMessage());
        }

        return matrixCursor;
    }


    private Cursor queryAll(String key, HashMap<String, String> map, int originPort, int wait) {
    //    Log.v("getall method","In getall method");
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        InputStream is;
        StringBuilder sb;
    //    Log.v("getall method",String.valueOf(thisPort));
    //    Log.v("getall method",String.valueOf(succPort));


        if (key.equals("@") || thisPort == succPort) {
     //       Log.v("getall method Key",key);
            String[] keyFiles = getContext().fileList();
            for(String file:keyFiles) {
          //      Log.v("getAll Method",file);
                sb = new StringBuilder();
                try {
            //        Log.v("getall Method","In try block of getall method");
                    is = getContext().openFileInput(file);
                    BufferedReader br = new BufferedReader(new InputStreamReader(is));
             //       Log.v("getall Method","After creating buffered reader");
                    String temp;
                    while ((temp = br.readLine()) != null) {
                        sb.append(temp);
                    }
              //      Log.v("getall Method","After appending data in buffered reader");
                    br.close();
               //     Log.v("String key",file);
              //      Log.v("String value",sb.toString());
                    matrixCursor.addRow(new String[] {file, sb.toString()});

                }
                catch (FileNotFoundException e) {
                    //  e.printStackTrace();
                    Log.e(TAG, " QueryTask UnknownHostException");
                }
                catch(IOException ie){
                    // ie.printStackTrace();
                    Log.e(TAG, "QueryTask IOException");
                }
                catch(Exception e){
                    //   e.printStackTrace();//general exception
                    Log.e(TAG, "QueryTask General Exception");

                }

            }
        //    Log.v("getAll method","returning cursor value");
            return matrixCursor;
        } else {
       //     Log.v("getAll method","If * query and not succ node");
            String[] keyFiles = getContext().fileList();

            for(String file:keyFiles) {
         //       Log.v("Key Value",file);
                sb = new StringBuilder();
                try {
                    is = getContext().openFileInput(file);
                    BufferedReader br = new BufferedReader(new InputStreamReader(is));

                    String temp;
                    while ((temp = br.readLine()) != null) {
                        sb.append(temp);
                    }
                    br.close();
              //      Log.v("getAll method","Putting data in map");
                    map.put(file,sb.toString());
                }

                catch (FileNotFoundException e) {
                    //  e.printStackTrace();
                    Log.e(TAG, " QueryTask UnknownHostException");
                }
                catch(IOException ie){
                    // ie.printStackTrace();
                    Log.e(TAG, "QueryTask IOException");
                }
                catch(Exception e){
                    //   e.printStackTrace();//general exception
                    Log.e(TAG, "QueryTask General Exception");


                }

            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(succPort),
                    key,null,String.valueOf(originPort),String.valueOf(0),"queryAll",String.valueOf(map));


            if (wait==1) {
                try {
                  //  Thread.sleep(100);
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Log.e(TAG,"Exception in sleep()");
                    e.printStackTrace();
                }
                for(Entry<String, String> entry : messageData.getMapData().entrySet()) {
                    matrixCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
                }
                messageData.setMapData(null);
            }
        }

        return matrixCursor;
    }




    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }


    //Server task
    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            ObjectInputStream inputStream;
            Socket clientSocket;
            MessageData message;



            try {
                while (true) {
                    clientSocket = serverSocket.accept();
                    inputStream = new ObjectInputStream(clientSocket.getInputStream());
                    message = (MessageData)inputStream.readObject();

                   //checking which method is called from join, accept, set_succ, insert, queryBykey, queryAll
                    if(message.getMessageType().equals("join")) {
                        Log.v("Server task ","join");
                        if (checkRing(message.getKey())) {
                            if (thisPort == predPort) {
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(message.getPredPort()),
                                        predIdHash,succIdHash,String.valueOf(predPort),String.valueOf(succPort),"accept",null);
                                //Setting predecessor and successor values
                                predIdHash = message.getKey();
                                predPort = message.getPredPort();
                                succIdHash = message.getKey();
                                succPort = message.getSuccPort();
                            } else {
                                //Calling client task for setting successor and accept
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(predPort),
                                        message.getKey(),null,String.valueOf(0),String.valueOf(message.getSuccPort()),"setSucc",null);

                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(message.getPredPort()),
                                        predIdHash,nodeIdHash,String.valueOf(predPort),String.valueOf(thisPort),"accept",null);

                                predIdHash = message.getKey();
                                predPort = message.getPredPort();
                            }
                        } else {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,String.valueOf(succPort),
                                    message.getKey(),message.getValue(),String.valueOf(message.getPredPort()),String.valueOf(message.getSuccPort()),
                                    message.getMessageType(), String.valueOf(message.getMapData()));
                        }
                    }
                    else if(message.getMessageType().equals("accept")) {
                        predIdHash = message.getKey();
                        Log.v("Server task accept","predIdHash"+predIdHash);
                        predPort = message.getPredPort();
                        Log.v("Server task accept","predPort"+predPort);
                        succIdHash = message.getValue();
                        Log.v("Server task accept","succIdHash"+succIdHash);
                        succPort = message.getSuccPort();
                        Log.v("Server task accept","succPort"+succPort);
                    }
                    else if(message.getMessageType().equals("insert")) {
                        Log.v("Server task ","insert");
                        insertData(message.getKey(), message.getValue());
                    }
                    else if(message.getMessageType().equals("setSucc")) {
                        Log.v("Server task ","setSucc");
                        succIdHash = message.getKey();
                        succPort = message.getSuccPort();
                    }
                    else if(message.getMessageType().equals("queryByKey")) {
                        Log.v("Server task ","queryByKey");
                        if (thisPort == message.getPredPort()) {
                            messageData.setValue(message.getValue());
                        } else {
                            queryByKey(message.getKey(), message.getPredPort(), 0);
                        }
                    }
                    else if(message.getMessageType().equals("queryAll")) {
                        Log.v("Server task ","queryAll");
                        if (thisPort == message.getPredPort()) {
                            messageData.setMapData(message.getMapData());
                        } else {
                            queryAll(message.getKey(), message.getMapData(), message.getPredPort(), 0);
                        }
                    }
                    else if(message.getMessageType().equals("delete")) {
                        Log.v("Server task ","delete");
                        deleteData(message.getKey(), message.getPredPort());
                    }
                    else{
                        Log.v(TAG, "Unknown message type: " + message.getMessageType());
                    }
                }
            }
            catch (UnknownHostException uhe) {
                Log.e(TAG, "ServerTask UnknownHostException: "+uhe.getMessage());

            }  catch (ClassNotFoundException cnfe) {
                Log.e(TAG, "ServerTask ClassNotFoundException: "+cnfe.getMessage());

            }
            catch (IOException ie) {
                Log.e(TAG, "ServerTask  IOException: " + ie.getMessage());

            }
            catch (Exception e) {
                Log.e(TAG, "ServerTask socket IOException: " + e.getMessage());
            }

            return null;
        }

    }

    // Referenced from-->  http://javatutorialhq.com/java/math/biginteger-class/compareto-method-example/
    private boolean checkRing(String keyHash) {
        BigInteger keyNode = new BigInteger(keyHash, 16);
        BigInteger predNode = new BigInteger(predIdHash, 16);
        BigInteger succNode = new BigInteger(nodeIdHash, 16);
    //    Log.v("checkRing","pred value"+predNode);
    //    Log.v("checkRing","key value"+keyNode);
    //    Log.v("checkRing","node  value"+succNode);

        if ((succNode.compareTo(predNode) == 0) ||
                (keyNode.compareTo(predNode) == 1 && keyNode.compareTo(succNode) < 1)) {
            return true;
        }

        if (predNode.compareTo(succNode) == 1) {
            if ((keyNode.compareTo(predNode) == 1 && keyNode.compareTo(succNode) == 1) ||
                    (keyNode.compareTo(predNode) == -1 && keyNode.compareTo(succNode) <= 0)){
                return true;
            }
        }

        return false;
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Log.v("ClientTask","Client task started");
            ObjectOutputStream outputStream;
            Socket socket;

            try {
              //  Log.v("Clienttask",msgs[0]);
                final int remote_port = Integer.parseInt(msgs[0]);
                final String key =msgs[1];
                final String value = msgs[2];
                final int predPort = Integer.parseInt(msgs[3]);
                final int succPort = Integer.parseInt(msgs[4]);
                final String messagetype = msgs[5];
                final String temp = msgs[6];
                HashMap<String, String> map = new HashMap<String, String>();
                if(temp!=null) {
                    //Reference from --> https://stackoverflow.com/questions/3957094/convert-hashmap-tostring-back-to-hashmap-in-java
                    Properties props = new Properties();
                    props.load(new StringReader(temp.substring(1, temp.length() - 1).replace(", ", "\n")));
                    map = new HashMap<String, String>();
                    for (HashMap.Entry<Object, Object> e : props.entrySet()) {
                        map.put((String) e.getKey(), (String) e.getValue());
                    }
                }
                else{
                    map = null;
                }
                Log.v("ClientTask","values"+remote_port+" "+key+" "+value+" "+predPort+" "+succPort+" "+messagetype+" "+temp);
                socket = new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2}), remote_port);
                //Adding the value to message object
                MessageData message = new MessageData(key, value, predPort, succPort, messagetype, map);
                outputStream = new ObjectOutputStream(socket.getOutputStream());
                outputStream.writeObject(message);
                outputStream.flush();
                socket.close();

            }
            catch (UnknownHostException uhe) {
                Log.e(TAG, "client Task Send UnknownHostException:"+uhe.getMessage());

            }
            catch (IOException ie) {
                Log.e(TAG, "clientTask IOException:"+ie.getMessage());
            }
            catch (Exception e) {
                Log.e(TAG, "Send socket IOException:"+e.getMessage());
            }
            return null;
        }

    }


    /**
     * Generates a SHA1 hash.
     *
     * @param input - the string the encode
     * @return SHA1 hash
     * @throws NoSuchAlgorithmException
     */
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

}