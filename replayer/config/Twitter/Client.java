package com.github.kiarahmani.replayer;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class Client {
	private Connection connect = null;
	private Statement stmt = null;
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

	/*
	 * 
	 * Insert Tweet
	 * 
	 */
	public void insertTweet(Long uid, String text, String time) throws Exception {
		try {
			PreparedStatement stmt0 = connect.prepareStatement("SELECT cid FROM TWEETS WHERE cuid=? ALLOW FILTERING");
			stmt0.setLong(1, uid);
			ResultSet rs0 = stmt0.executeQuery();
			if (!rs0.next()) {
				System.out.println("tweet does not exist! " + uid);
			}

			long oldId = rs0.getLong("cid");
			PreparedStatement stmt1 = connect
					.prepareStatement("INSERT INTO TWEETS (cid, cuid,ctext,createdate) VALUES (?, ?, ?, ?)");
			stmt1.setLong(1, oldId + 1);
			stmt1.setLong(2, uid);
			stmt1.setString(3, text);
			stmt1.setString(4, time);
			stmt1.executeUpdate();
		} catch (Exception e) {
			throw e;
		} finally {
		}
	}

}
