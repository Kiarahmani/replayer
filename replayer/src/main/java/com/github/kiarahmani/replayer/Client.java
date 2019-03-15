package com.github.kiarahmani.replayer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

	private void close() {
		try {
			connect.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void firstTXN(Long key) throws Exception {

		Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
		try {
			connect = DriverManager.getConnection("jdbc:cassandra://localhost" + ":1904" + this.id + "/testks");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("successfully connected to Cassandra! \n" + connect);
		preparedStatement = connect.prepareStatement("SELECT balance FROM accounts WHERE id = ?");
		preparedStatement.setInt(1, (int) (long) key);
		rs = preparedStatement.executeQuery();
		if (rs.next()) {
			int balance = rs.getInt("balance");
			System.out.println("the balance for the read account is: " + balance);
		}
		close();

	}

	public void seocndTXN(Long id, Long anotherId) {
		for (int i = 0; i < 50; i++) {
			System.out.println(i + ": I am transactionTwo! id:" + id);
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void thirdTXN(Long id, String text1, Long someLong) {
		for (int i = 0; i < 50; i++) {
			System.out.println(i + ": I am transactionTwo! id:" + id);
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
