package com.github.kiarahmani.replayer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Client {
	private Connection connect = null;
	private Statement stmt = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet rs = null;
	private int _ISOLATION = Connection.TRANSACTION_READ_UNCOMMITTED;
	private int id;
	Properties p;

	public Client(int id) {
		this.id = id;
		p = new Properties();
		p.setProperty("id", String.valueOf(this.id));
	}

	public long transactionOne(int id) {
		for (int i = 0; i < 800; i++) {
			System.out.println(i+": I am transactionOne! id:" + id);
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}
	
	
	
	
	public long transactionTwo(int id) {
		for (int i = 0; i < 800; i++) {
			System.out.println(i+": I am transactionTwo! id:" + id);
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

}
