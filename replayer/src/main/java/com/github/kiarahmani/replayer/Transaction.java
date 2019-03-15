package com.github.kiarahmani.replayer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Transaction {
	private Client client;
	private int id;

	// constructor
	public Transaction(int id) {
		this.id = id;
		client = new Client(id);
	}

	// run the transaction wrapper
	public void run(String opType) {
		try {
			Method transaction = client.getClass().getMethod(opType, int.class);
			long time = (Long) transaction.invoke(client, this.id);
			System.out.println("Execution time (" + opType + "): " + time);
		} catch (NoSuchMethodException e2) {
			System.err.println("Unknown Operation Type: " + opType);
			e2.printStackTrace();
		} catch (SecurityException e2) {
			e2.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
}
