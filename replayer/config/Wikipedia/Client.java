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

	
	
	
	public void getPageAuthenticated(Boolean forSelect, String userIp, Long userId, Long nameSpace, String pageTitle)
			throws SQLException {

		if (userId >= 0) {
			PreparedStatement st = connect
					.prepareStatement("SELECT * FROM " + "useracct" + " WHERE user_id = ?  ALLOW FILTERING");
			st.setLong(1, userId);
			ResultSet rs = st.executeQuery();
			if (!rs.next())
				System.out.println("Invalid UserId: " + userId);
			String userText = rs.getString("user_name");
			rs.close();
			// Fetch all groups the user might belong to (access control // information)
			PreparedStatement st1 = connect
					.prepareStatement("SELECT ug_group FROM " + "USER_GROUPS" + " WHERE ug_user = ? ALLOW FILTERING");
			st1.setLong(1, userId);
			ResultSet rs1 = st1.executeQuery();
			while (rs1.next()) {

				@SuppressWarnings("unused")
				String userGroupName = rs1.getString(1);
			}
			rs1.close();

			PreparedStatement st2 = connect.prepareStatement(
					"SELECT * FROM " + "PAGE" + " WHERE page_namespace = ? AND page_title = ? ALLOW FILTERING");
			st2.setLong(1, nameSpace);
			st2.setString(2, pageTitle);
			ResultSet rs2 = st2.executeQuery();
			if (!rs2.next()) {
				String msg = String.format("Invalid Page: Namespace:%d / Title:--%s--", nameSpace, pageTitle);
				System.out.println(msg);
			}
			int pageId = rs2.getInt("page_id");
			int page_latest = rs2.getInt("page_latest");

			PreparedStatement st3 = connect
					.prepareStatement("SELECT * FROM " + "PAGE_RESTRICTIONS" + " WHERE pr_page = ?");
			st3.setInt(1, pageId);
			ResultSet rs3 = st3.executeQuery();
			while (rs3.next()) {
				byte[] pr_type = rs3.getBytes("pr_type");
				assert (pr_type != null);
			}
			// check using blockin of a user by either the IP address or the
			// user_name
			PreparedStatement st4 = connect.prepareStatement("SELECT * FROM " + "IPBLOCKS" + " WHERE ipb_address = ?");
			st4.setString(1, userIp);
			ResultSet rs4 = st4.executeQuery();
			while (rs4.next()) {
				byte[] ipb_expiry = rs4.getBytes(11);
				assert (ipb_expiry != null);
			} // WHILE

			PreparedStatement st5 = connect
					.prepareStatement("SELECT * " + "FROM " + "REVISION" + " WHERE rev_page = ? AND rev_id = ?");
			st5.setInt(1, pageId);
			st5.setInt(2, page_latest);
			ResultSet rs5 = st5.executeQuery();
			if (!rs5.next()) {
				String msg = String.format("Invalid Page: Namespace:%d / Title:--%s-- / PageId:%d", nameSpace,
						pageTitle, pageId);
				System.out.println(msg);
			}

			int revisionId = rs5.getInt("rev_id");
			int textId = rs5.getInt("rev_text_id");

			//
			PreparedStatement st6 = connect
					.prepareStatement("SELECT old_text, old_flags FROM " + "TEXT" + " WHERE old_id = ? LIMIT 1");
			st6.setInt(1, textId);
			ResultSet rs6 = st6.executeQuery();
			if (!rs6.next()) {
				String msg = "No such text: " + textId + " for page_id:" + pageId + " page_namespace: " + nameSpace
						+ " page_title:" + pageTitle;
				System.out.println(msg);
			}
			if (!forSelect)
				System.out.println("userText:" + userText + " pageId:" + pageId + "old_text: "
						+ rs6.getString("old_text") + " textId:" + textId + " revisionId:" + revisionId);
			assert !rs6.next();
		}

		if (userId < 0) {

			PreparedStatement st2 = connect.prepareStatement(
					"SELECT * FROM " + "PAGE" + " WHERE page_namespace = ? AND page_title = ? ALLOW FILTERING");
			st2.setLong(1, nameSpace);
			st2.setString(2, pageTitle);
			ResultSet rs2 = st2.executeQuery();
			if (!rs2.next()) {
				String msg = String.format("Invalid Page: Namespace:%d / Title:--%s--", nameSpace, pageTitle);
				System.out.println(msg);
			}
			int pageId = rs2.getInt("page_id");
			int page_latest = rs2.getInt("page_latest");

			PreparedStatement st3 = connect
					.prepareStatement("SELECT * FROM " + "PAGE_RESTRICTIONS" + " WHERE pr_page = ? ALLOW FILTERING" );
			st3.setInt(1, pageId);
			ResultSet rs3 = st3.executeQuery();
			while (rs3.next()) {
				String pr_type = rs3.getString("pr_type");
			}
			// check using blocking of a user by either the IP address or the // user_name
			PreparedStatement st4 = connect.prepareStatement("SELECT * FROM " + "IPBLOCKS" + " WHERE ipb_address = ?  ALLOW FILTERING");
			st4.setString(1, userIp);
			ResultSet rs4 = st4.executeQuery();
			while (rs4.next()) {
				String  ipb_expiry = rs4.getString(11);
			} // WHILE

			PreparedStatement st5 = connect
					.prepareStatement("SELECT * " + "FROM " + "REVISION" + " WHERE rev_page = ? AND rev_id = ?  ALLOW FILTERING");
			st5.setInt(1, pageId);
			st5.setInt(2, page_latest);
			ResultSet rs5 = st5.executeQuery();
			if (!rs5.next()) {
				String msg = String.format("Invalid Page: Namespace:%d / Title:--%s-- / PageId:%d", nameSpace,
						pageTitle, pageId);
				System.out.println(msg);
			}

			int revisionId = rs5.getInt("rev_id");
			int textId = rs5.getInt("rev_text_id");

			//
			PreparedStatement st6 = connect
					.prepareStatement("SELECT old_text, old_flags FROM " + "TEXT" + " WHERE old_id = ? ALLOW FILTERING");
			st6.setInt(1, textId);
			ResultSet rs6 = st6.executeQuery();
			if (!rs6.next()) {
				String msg = "No such text: " + textId + " for page_id:" + pageId + " page_namespace: " + nameSpace
						+ " page_title:" + pageTitle;
				System.out.println(msg);
			}
			if (!forSelect)
				System.out.println("userText:" + userIp + " pageId:" + pageId + "old_text: " + rs6.getString("old_text")
						+ " textId:" + textId + " revisionId:" + revisionId);
			assert !rs6.next();
		}
	}
	
	
	
	/*
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 */
	
	
	public void UpdatePage(Long textId, Long pageId, String pageTitle, String pageText, Long pageNamespace, Long userId,
			String userIp, String userText, Long revisionId, String revComment, Long revMinorEdit, String timestamp)
			throws SQLException {

		PreparedStatement ps = connect.prepareStatement("SELECT old_id FROM TEXT WHERE old_page = ? ALLOW FILTERING");
		ps.setLong(1, pageId);
		ResultSet rs = ps.executeQuery();
		if (!rs.next()) {
			System.out.println("invalid page id: " + pageId);
			return;
		}
		int old_id = rs.getInt("old_id");

		PreparedStatement ps1 = connect.prepareStatement(
				"INSERT INTO " + "TEXT" + " (" + "old_id,old_text,old_flags,old_page" + ") VALUES (" + "?,?,?,?" + ")");
		ps1.setInt(1, old_id + 1);
		ps1.setString(2, pageText);
		ps1.setString(3, "utf-8");
		ps1.setLong(4, pageId);
		ps1.executeUpdate();

		// INSERT NEW REVISION
		PreparedStatement ps20 = connect.prepareStatement("SELECT rev_id FROM REVISION WHERE rev_page = ? ALLOW FILTERING");
		ps20.setLong(1, pageId);
		ResultSet rs1 = ps20.executeQuery();
		if (!rs1.next()) {
			System.out.println("Invalid pageId:" + pageId);
			return;
		}
		int old_rev_id = rs1.getInt("rev_id");

		PreparedStatement ps21 = connect.prepareStatement(
				"INSERT INTO " + "REVISION" + " (" + "rev_id, rev_page, " + "rev_text_id, " + "rev_comment, "
						+ "rev_user, " + "rev_user_text, " + "rev_timestamp, " + "rev_minor_edit, " + "rev_deleted, "
						+ "rev_len, " + "rev_pcarenct_id" + ") VALUES (" + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" + ")");
		ps21.setInt(1, old_rev_id + 1);
		ps21.setLong(2, pageId); // rev_page
		ps21.setInt(3, old_id + 1); // rev_text_id
		ps21.setString(4, revComment);// rev_comment
		ps21.setLong(5, userId); // rev_user
		ps21.setString(6, userText); // rev_user_text
		ps21.setString(7, timestamp); // rev_timestamp
		ps21.setLong(8, revMinorEdit); // rev_minor_edit // this is an error
		ps21.setInt(9, 0); // rev_deleted //this is an error
		ps21.setInt(10, pageText.length()); // rev_len
		ps21.setLong(11, revisionId); // rev_parent_id // this is an error
		ps21.executeUpdate();

		PreparedStatement ps3 = connect.prepareStatement("UPDATE " + "PAGE"
				+ "   SET page_latest = ?, page_touched = ?, page_is_new = 0, page_is_redirect = 0, page_len = ? "
				+ " WHERE page_id = ?");
		ps3.setInt(1, old_rev_id + 1);
		ps3.setString(2, timestamp);
		ps3.setInt(3, pageText.length());
		ps3.setLong(4, pageId);
		ps3.executeUpdate();

		PreparedStatement ps4 = connect.prepareStatement("INSERT INTO " + "RECENTCHANGES" + " (" + "rc_timestamp, "
				+ "rc_cur_time, " + "rc_namespace, " + "rc_title, " + "rc_type, " + "rc_minor, " + "rc_cur_id, "
				+ "rc_user, " + "rc_user_text, " + "rc_comment, " + "rc_this_oldid, " + "rc_last_oldid, " + "rc_bot, "
				+ "rc_moved_to_ns, " + "rc_moved_to_title, " + "rc_ip, " + "rc_old_len, " + "rc_new_len, rc_id" + ") VALUES ("
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" + ")");

		ps4.setString(1, timestamp); // rc_timestamp
		ps4.setString(2, timestamp); // rc_cur_time
		ps4.setLong(3, pageNamespace); // rc_namespace
		ps4.setString(4, pageTitle); // rc_title
		ps4.setInt(5, 0); // rc_type
		ps4.setInt(6, 0); // rc_minor
		ps4.setLong(7, pageId); // rc_cur_id
		ps4.setLong(8, userId); // rc_user
		ps4.setString(9, userText); // rc_user_text
		ps4.setString(10, revComment); // rc_comment
		ps4.setInt(11, old_id + 1); // rc_this_oldid
		ps4.setLong(12, textId); // rc_last_oldid
		ps4.setInt(13, 0); // rc_bot
		ps4.setInt(14, 0); // rc_moved_to_ns
		ps4.setString(15, ""); // rc_moved_to_title
		ps4.setString(16, userIp); // rc_ip
		ps4.setInt(17, pageText.length());// rc_old_len
		ps4.setInt(18, pageText.length());// rc_new_len
		ps4.setInt(19, 999999);
		ps4.executeUpdate();

		// SELECT WATCHING USERS
		PreparedStatement ps5 = connect.prepareStatement("SELECT wl_user FROM " + "WATCHLIST" + " WHERE wl_title = ?"
				+ "   AND wl_namespace = ?");

		ps5.setString(1, pageTitle);
		ps5.setLong(2, pageNamespace);
		ResultSet rs5 = ps5.executeQuery();

		while (rs5.next()) {
			int wlUser = rs5.getInt(1);
			PreparedStatement ps6 = connect
					.prepareStatement("UPDATE " + "WATCHLIST" + "   SET wl_notificationtimestamp = ? "
							+ " WHERE wl_title = ?" + "   AND wl_namespace = ?" + "   AND wl_user = ?");
			ps6.setString(1, timestamp);
			ps6.setString(2, pageTitle);
			ps6.setLong(3, pageNamespace);
			ps6.setInt(4, wlUser);
			ps6.executeUpdate();

			PreparedStatement ps7 = connect.prepareStatement("SELECT * FROM " + "useracct" + " WHERE user_id = ?");
			ps7.setInt(1, wlUser);
			ResultSet rs7 = ps7.executeQuery();
			rs7.next();
		}

	}

	/*
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 */
	
	
	public void UpdatePageLog(Long textId, Long pageId, String pageTitle, String pageText, Long pageNamespace, Long userId,
			String userIp, String userText, Long revisionId, String revComment, Long revMinorEdit, String timestamp)
			throws Exception {
		try {
			PreparedStatement ps8 = connect.prepareStatement(
					"INSERT INTO " + "LOGGING" + " (" + "log_id, log_type, log_action, log_timestamp, log_user, log_user_text, "
							+ "log_namespace, log_title, log_page, log_comment, log_params" + ") VALUES ("
							+ "?,?,?,?,?,?,?,?,?,?,?" + ")");
			ps8.setInt(1, 1);
			ps8.setString(2, "patrol");
			ps8.setString(3, "patrol");
			ps8.setString(4, timestamp);
			ps8.setLong(5, userId);
			ps8.setString(6, pageTitle);
			ps8.setLong(7, pageNamespace);
			ps8.setString(8, userText);
			ps8.setLong(9, pageId);
			ps8.setString(10, "");
			ps8.setString(11, String.format("%d\n%d\n%d", 1, revisionId, 1));
			ps8.executeUpdate();

			PreparedStatement ps90 = connect
					.prepareStatement("SELECT user_editcount FROM  " + "useracct" + " WHERE user_id = ?");
			ps90.setLong(1, userId);
			ResultSet rs90 = ps90.executeQuery();
			rs90.next();
			int old_user_editcount = rs90.getInt("user_editcount");

			PreparedStatement ps91 = connect
					.prepareStatement("UPDATE " + "useracct" + "   SET user_editcount=?" + " WHERE user_id = ?");
			ps91.setInt(1, old_user_editcount);
			ps91.setLong(2, userId);
			ps91.executeUpdate();

			PreparedStatement ps100 = connect
					.prepareStatement("UPDATE " + "useracct" + "   SET user_touched=?" + " WHERE user_id = ?");
			ps100.setString(1, "now");
			ps100.setLong(2, userId);
			ps100.executeUpdate();
		} catch (Exception e) {
			throw e;
		} finally {
		}
	}

}
