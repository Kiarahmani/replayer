package com.github.kiarahmani.replayer;

import java.sql.Connection;
import java.sql.Driver;
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
		Object o;
		try {
			o = Class.forName("MyDriver").newInstance();
			System.out.println("+++" + o);
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect("", p);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	private void close() {
		try {
			connect.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void deposit(Long key, Long amount) throws Exception {
		preparedStatement = connect.prepareStatement("SELECT balance FROM accounts WHERE id = ?");
		preparedStatement.setInt(1, (int) (long) key);
		rs = preparedStatement.executeQuery();
		if (!rs.next())
			return;
		int balance = rs.getInt("balance");
		System.out.println("intial balance for the account #" + key + " is: " + balance);
		//
		preparedStatement = connect.prepareStatement("UPDATE accounts SET balance = ? WHERE id = ?");
		preparedStatement.setInt(1, balance + (int) (long) amount);
		preparedStatement.setInt(2, (int) (long) key);
		preparedStatement.executeUpdate();

		close();
	}

	public void withdraw(Long key, Long amount) throws SQLException {
		preparedStatement = connect.prepareStatement("SELECT balance FROM accounts WHERE id = ?");
		preparedStatement.setInt(1, (int) (long) key);
		rs = preparedStatement.executeQuery();
		if (!rs.next())
			return;
		int balance = rs.getInt("balance");
		System.out.println("intial balance for the account #" + key + " is: " + balance);
		//
		preparedStatement = connect.prepareStatement("UPDATE accounts SET balance = ? WHERE id = ?");
		preparedStatement.setInt(1, balance - (int) (long) amount);
		preparedStatement.setInt(2, (int) (long) key);
		preparedStatement.executeUpdate();

		close();
	}

}
