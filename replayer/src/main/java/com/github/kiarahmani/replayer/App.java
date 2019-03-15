package com.github.kiarahmani.replayer;

public class App 
{
    public static void main( String[] args )
    {
    	String txnType = args[0];
		int id = Integer.valueOf(args[1]);
		Transaction txn = new Transaction(id);
		txn.run(txnType);
    }
}
