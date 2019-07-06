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

	// ***********************************************************************************
		//
		//
		//
		//
		//
		//
		//
		//
		// ************************************************************************************
	public void orderStatus(Long w_id, Long d_id, Long customerByName, Long c_id, String c_last) throws Exception {
		try {
			PreparedStatement stmt = null;
			ResultSet c_rs = null;
			if (customerByName==0) {
				stmt = connect.prepareStatement("SELECT C_ID" + "  FROM " + "CUSTOMER" + " WHERE C_W_ID = ? "
						+ "   AND C_D_ID = ? " + "   AND C_LAST = ? " + "ALLOW FILTERING");
				stmt.setLong(1, w_id);
				stmt.setLong(2, d_id);
				stmt.setString(3, c_last);
				c_rs = stmt.executeQuery();
				// find the appropriate index
				int index = 0;
				List<Long> all_c_ids = new ArrayList<Long>();
				while (c_rs.next()) {
					index++;
					all_c_ids.add(c_rs.getLong("C_ID"));
				}
				if (index == 0) {
					System.out.println(
							"ERROR_23: No customer with the given last name: " + w_id + "," + d_id + "," + c_last);
				}
				if (index % 2 != 0)
					index++;
				index = (index / 2);
				c_id = all_c_ids.get(index - 1);
				c_rs.close();
			}
			// now retrive the customer's data based on the given c_id (or the chosen one in
			// case of customerByName)
			stmt = connect.prepareStatement("SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE" + " FROM CUSTOMER"
					+ " WHERE C_W_ID = ? " + " AND C_D_ID = ? " + " AND C_ID = ?");
			stmt.setLong(1, w_id);
			stmt.setLong(2, d_id);
			stmt.setLong(3, c_id);
			c_rs = stmt.executeQuery();
			String c_first = c_rs.getString("C_FIRST");
			String c_middle = c_rs.getString("C_MIDDLE");
			c_last = c_rs.getString("C_LAST");
			double c_balance = c_rs.getDouble("C_BALANCE");
			c_rs.close();
			//
			// find the newest order for the customer
			// retrieve the carrier & order date for the most recent order.
			stmt = connect.prepareStatement("SELECT MAX(O_ID) " + "  FROM " + "OORDER" + " WHERE O_W_ID = ? "
					+ "   AND O_D_ID = ? " + "AND O_C_ID = ? " + "ALLOW FILTERING");
			stmt.setLong(1, w_id);
			stmt.setLong(2, d_id);
			stmt.setLong(3, c_id);
			ResultSet o_rs = stmt.executeQuery();
			int o_id = o_rs.getInt(1);
			o_rs.close();
			stmt = connect.prepareStatement("SELECT  O_CARRIER_ID, O_ENTRY_D  " + "  FROM " + "OORDER"
					+ " WHERE O_W_ID = ? " + "   AND O_D_ID = ? " + "AND O_ID = ? ");
			stmt.setLong(1, w_id);
			stmt.setLong(2, d_id);
			stmt.setInt(3, o_id);
			o_rs = stmt.executeQuery();
			if (!o_rs.next()) {
				System.out.println(String.format(
						"ERROR_31: No order records for CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_ID=%d]", w_id, d_id, c_id));
			}
			int o_carrier_id = o_rs.getInt("O_CARRIER_ID");
			Timestamp o_entry_d = o_rs.getTimestamp("O_ENTRY_D");
			o_rs.close();
			// retrieve the order lines for the most recent order
			stmt = connect.prepareStatement("SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D "
					+ "  FROM " + "ORDER_LINE" + " WHERE OL_O_ID = ?" + "   AND OL_D_ID = ?" + "   AND OL_W_ID = ?");
			stmt.setInt(1, o_id);
			stmt.setLong(2, d_id);
			stmt.setLong(3, w_id);
			ResultSet ol_rs = stmt.executeQuery();

			// craft the final result
			ArrayList<String> orderLines = new ArrayList<String>();
			while (ol_rs.next()) {
				StringBuilder sb = new StringBuilder();
				sb.append("[");
				sb.append(ol_rs.getInt("OL_SUPPLY_W_ID"));
				sb.append(" - ");
				sb.append(ol_rs.getInt("OL_I_ID"));
				sb.append(" - ");
				sb.append(ol_rs.getDouble("OL_QUANTITY"));
				sb.append(" - ");
				sb.append(String.valueOf(ol_rs.getDouble("OL_AMOUNT")));
				sb.append(" - ");
				if (ol_rs.getTimestamp("OL_DELIVERY_D") != null)
					sb.append(ol_rs.getTimestamp("OL_DELIVERY_D"));
				else
					sb.append("99-99-9999");
				sb.append("]");
				orderLines.add(sb.toString());
			}

			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			// TXN SUCCESSFUL!
			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	// ***********************************************************************************
		//
		//
		//
		//
		//
		//
		//
		//
		// ************************************************************************************
	public void delivery(Long w_id, Long o_carrier_id) throws Exception {
		try {
			PreparedStatement stmt = null;
			PreparedStatement ol_stmt = null;
			PreparedStatement no_stmt = null;
			PreparedStatement oo_stmt = null;
			PreparedStatement cu_stmt = null;

			int d_id;
			int[] orderIDs = new int[120];
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			for (d_id = 1; d_id <= 10; d_id++) {
				d_id = 16;
				stmt = connect.prepareStatement("SELECT NO_O_ID FROM " + "NEW_ORDER" + " WHERE NO_D_ID = ? "
						+ "   AND NO_W_ID = ? " + " ORDER BY no_d_id,no_o_id" + " LIMIT 1 ALLOW FILTERING");
				stmt.setInt(1, d_id);
				stmt.setLong(2, w_id);
				ResultSet no_rs = stmt.executeQuery();
				if (!no_rs.next()) {
					// This district has no new orders
					// This can happen but should be rare
					System.out.println(String.format("District has no new orders [W_ID=%d, D_ID=%d]", w_id, d_id));
				}

				int no_o_id = no_rs.getInt("NO_O_ID");
				orderIDs[d_id - 1] = no_o_id;
				no_rs.close();
				//
				// delete the row containing the oldest order
				no_stmt = connect.prepareStatement("DELETE FROM " + "NEW_ORDER" + " WHERE NO_O_ID = ? "
						+ " AND NO_D_ID = ?" + "   AND NO_W_ID = ?");

				no_stmt.setInt(1, no_o_id);
				no_stmt.setInt(2, d_id);
				no_stmt.setLong(3, w_id);
				no_stmt.executeUpdate();

				// retrieve order
				stmt = connect.prepareStatement("SELECT O_C_ID FROM " + "OORDER" + " WHERE O_ID = ? "
						+ "   AND O_D_ID = ? " + "   AND O_W_ID = ?");
				stmt.setInt(1, no_o_id);
				stmt.setInt(2, d_id);
				stmt.setLong(3, w_id);
				ResultSet oo_rs = stmt.executeQuery();
				if (!oo_rs.next()) {
					System.out.println(
							String.format("ERROR_41: Failed to retrieve ORDER record [W_ID=%d, D_ID=%d, O_ID=%d]", w_id,
									d_id, no_o_id));
				}
				int c_id = oo_rs.getInt("O_C_ID");
				oo_rs.close();

				// update order's carrier id
				oo_stmt = connect.prepareStatement("UPDATE OORDER  SET O_CARRIER_ID = ? " + " WHERE O_ID = ? "
						+ "   AND O_D_ID = ?" + "   AND O_W_ID = ?");
				oo_stmt.setLong(1, o_carrier_id);
				oo_stmt.setInt(2, no_o_id);
				oo_stmt.setInt(3, d_id);
				oo_stmt.setLong(4, w_id);
				oo_stmt.executeUpdate();

	
				//
				// retrieve all orderlines belonging to this order
				stmt = connect.prepareStatement("SELECT OL_NUMBER, OL_AMOUNT FROM ORDER_LINE " + " WHERE OL_O_ID = ? "
						+ "   AND OL_D_ID = ? " + "   AND OL_W_ID = ? ");
				stmt.setInt(1, no_o_id);
				stmt.setInt(2, d_id);
				stmt.setLong(3, w_id);
				ResultSet ol_rs = stmt.executeQuery();
				List<Integer> all_ol_numbers = new ArrayList<Integer>();
				// read all ol_numbers and get sum of ol_amounts
				double ol_total = 0;
				while (ol_rs.next()) {
					all_ol_numbers.add(ol_rs.getInt("OL_NUMBER"));
					ol_total += ol_rs.getDouble("OL_AMOUNT");
				}

				// update all matching rows in orderline table
				String ol_in_clause = "(69)";
				ol_stmt = connect
						.prepareStatement("UPDATE " + "ORDER_LINE" + "   SET OL_DELIVERY_D = ? " + " WHERE OL_O_ID = ? "
								+ "   AND OL_D_ID = ? " + "   AND OL_W_ID = ? " + "AND OL_NUMBER IN " + ol_in_clause);
				ol_stmt.setTimestamp(1, timestamp);
				ol_stmt.setInt(2, no_o_id);
				ol_stmt.setInt(3, d_id);
				ol_stmt.setLong(4, w_id);
				ol_stmt.executeUpdate();


				// retrieve customer's info
				stmt = connect.prepareStatement("SELECT  C_BALANCE, C_DELIVERY_CNT" + " FROM CUSTOMER"
						+ " WHERE C_W_ID = ? " + "   AND C_D_ID = ? " + " AND C_ID = ? ");
				stmt.setLong(1, w_id);
				stmt.setInt(2, d_id);
				stmt.setInt(3, c_id);
				ResultSet c_rs = stmt.executeQuery();
				if (!c_rs.next()) {
					System.out.println("ERROR_42: customer does not exist: " + w_id + "," + d_id + "," + c_id);
				}
				double c_balance = c_rs.getDouble("C_BALANCE");
				int c_delivery_cnt = c_rs.getInt("C_DELIVERY_CNT");
				c_rs.close();
				// update customer's info
				cu_stmt = connect.prepareStatement("UPDATE " + "CUSTOMER" + " SET C_BALANCE = ?,"
						+ " C_DELIVERY_CNT = ? " + " WHERE C_W_ID = ? " + "   AND C_D_ID = ? " + " AND C_ID = ? ");
				cu_stmt.setDouble(1, c_balance + ol_total);
				cu_stmt.setInt(2, c_delivery_cnt + 1);
				cu_stmt.setLong(3, w_id);
				cu_stmt.setInt(4, d_id);
				cu_stmt.setInt(5, c_id);
				cu_stmt.executeUpdate();
			}
			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			// TXN SUCCESSFUL!
			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	// ***********************************************************************************
		//
		//
		//
		//
		//
		//
		//
		//
		// ************************************************************************************
	public void newOrder(Long w_id, Long d_id, Long c_id, Long o_all_local, Long o_ol_cnt) throws Exception {
		PreparedStatement stmt = null, stmtUpdateStock = null;

		Long[] orderQuantities = { 1L, 1L, 1L, 1L, 1L };
		Long[] supplierWarehouseIDs = { w_id, w_id, w_id, w_id, w_id };
		Long[] itemIDs = { 31L, 31L, 31L, 31L, 31L };

		try {

			// datastructures required for bookkeeping
			double[] itemPrices = new double[(int) (long) o_ol_cnt];
			String[] itemNames = new String[(int) (long) o_ol_cnt];
			double[] stockQuantities = new double[(int) (long) o_ol_cnt];
			double[] orderLineAmounts = new double[(int) (long) o_ol_cnt];
			char[] brandGeneric = new char[(int) (long) o_ol_cnt];

			// retrieve w_tax rate
			stmt = connect.prepareStatement("SELECT W_TAX " + "  FROM " + "WAREHOUSE" + " WHERE W_ID = ?");
			stmt.setLong(1, w_id);
			ResultSet w_rs = stmt.executeQuery();
			if (!w_rs.next()) {
				System.out.println("ERROR_11: Invalid warehouse id: " + w_id);
			}
			double w_tax = w_rs.getDouble("W_TAX");
			w_rs.close();
			//
			// retrieve d_tax rate and update D_NEXT_O_ID
			stmt = connect.prepareStatement(
					"SELECT D_NEXT_O_ID, D_TAX " + "  FROM " + "DISTRICT" + " WHERE D_W_ID = ? AND D_ID = ?");
			stmt.setLong(1, w_id);
			stmt.setLong(2, d_id);
			ResultSet d_rs = stmt.executeQuery();
			if (!d_rs.next()) {
				System.out.println("ERROR_12: Invalid district id: (" + w_id + "," + d_id + ")");
			}
			int d_next_o_id = d_rs.getInt("D_NEXT_O_ID");
			double d_tax = d_rs.getDouble("D_TAX");

			stmt = connect.prepareStatement(
					"UPDATE " + "DISTRICT" + "   SET D_NEXT_O_ID = ? " + " WHERE D_W_ID = ? " + "   AND D_ID = ?");
			stmt.setInt(1, d_next_o_id + 1);
			stmt.setLong(2, w_id);
			stmt.setLong(3, d_id);
			stmt.executeUpdate();
			int o_id = d_next_o_id;
			//
			// insert a new row into OORDER and NEW_ORDER tables
			stmt = connect.prepareStatement(
					"INSERT INTO " + "OORDER" + " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)"
							+ " VALUES (?, ?, ?, ?, ?, ?, ?)");
			stmt.setInt(1, o_id);
			stmt.setLong(2, d_id);
			stmt.setLong(3, w_id);
			stmt.setLong(4, c_id);
			stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
			stmt.setLong(6, o_ol_cnt);
			stmt.setLong(7, o_all_local);
			stmt.executeUpdate();
			//
			stmt = connect.prepareStatement(
					"INSERT INTO " + "NEW_ORDER" + " (NO_O_ID, NO_D_ID, NO_W_ID) " + " VALUES ( ?, ?, ?)");
			stmt.setInt(1, o_id);
			stmt.setLong(2, d_id);
			stmt.setLong(3, w_id);
			stmt.executeUpdate();

			//
			// retrieve customer's information
			stmt = connect.prepareStatement("SELECT C_DISCOUNT, C_LAST, C_CREDIT" + "  FROM " + "CUSTOMER"
					+ " WHERE C_W_ID = ? " + "   AND C_D_ID = ? " + "   AND C_ID = ?");
			stmt.setLong(1, w_id);
			stmt.setLong(2, d_id);
			stmt.setLong(3, c_id);
			ResultSet c_rs = stmt.executeQuery();
			if (!c_rs.next()) {
				System.out.println("ERROR_13: Invalid customer id: (" + w_id + "," + d_id + "," + c_id + ")");
			}
			double c_discount = c_rs.getDouble("C_DISCOUNT");
			String c_last = c_rs.getString("C_LAST");
			String c_credit = c_rs.getString("C_CREDIT");
			double total_amount = 0;
			// System.out.println("=======");
			// For each O_OL_CNT item on the order perform the following tasks
			for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
				Long ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
				Long ol_i_id = itemIDs[ol_number - 1];
				Long ol_quantity = orderQuantities[ol_number - 1];
				// retrieve item
				stmt = connect
						.prepareStatement("SELECT I_PRICE, I_NAME , I_DATA " + "  FROM " + "ITEM" + " WHERE I_ID = ?");
				stmt.setLong(1, ol_i_id);
				ResultSet i_rs = stmt.executeQuery();
				// this is expected to happen 1% of the times
				if (!i_rs.next()) {
					if (ol_number != o_ol_cnt) {
						System.out.println("ERROR_14: Invalid item id: (" + ol_i_id
								+ ") given in the middle of the order list (unexpected)");
					}
					System.out.println("EXPECTED_ERROR_15: Invalid item id: (" + ol_i_id + ")");
				}
				double i_price = i_rs.getDouble("I_PRICE");
				String i_name = i_rs.getString("I_NAME");
				String i_data = i_rs.getString("I_DATA");
				i_rs.close();
				itemPrices[ol_number - 1] = i_price;
				itemNames[ol_number - 1] = i_name;

				// retrieve stock
				stmt = connect
						.prepareStatement("SELECT  *  FROM " + "STOCK" + " WHERE S_I_ID = ? " + "   AND S_W_ID = ?");
				stmt.setLong(1, ol_i_id);
				stmt.setLong(2, ol_supply_w_id);
				ResultSet s_rs = stmt.executeQuery();
				if (!s_rs.next()) {
					System.out.println("ERROR_16: Invalid stock primary key: (" + ol_i_id + "," + ol_supply_w_id + ")");
				}
				double s_quantity = s_rs.getDouble("S_QUANTITY");
				double s_ytd = s_rs.getDouble("S_YTD");
				int s_order_cnt = s_rs.getInt("S_ORDER_CNT");
				int s_remote_cnt = s_rs.getInt("S_REMOTE_CNT");
				String s_data = s_rs.getString("S_DATA");
				String s_dist_01 = s_rs.getString("S_DIST_01");
				String s_dist_02 = s_rs.getString("S_DIST_02");
				String s_dist_03 = s_rs.getString("S_DIST_03");
				String s_dist_04 = s_rs.getString("S_DIST_04");
				String s_dist_05 = s_rs.getString("S_DIST_05");
				String s_dist_06 = s_rs.getString("S_DIST_06");
				String s_dist_07 = s_rs.getString("S_DIST_07");
				String s_dist_08 = s_rs.getString("S_DIST_08");
				String s_dist_09 = s_rs.getString("S_DIST_09");
				String s_dist_10 = s_rs.getString("S_DIST_10");
				s_rs.close();
				//
				stockQuantities[ol_number - 1] = s_quantity;
				if (s_quantity - ol_quantity >= 10) {
					s_quantity -= ol_quantity; // new s_quantity
				} else {
					s_quantity += -ol_quantity + 91; // new s_quantity
				}
				int s_remote_cnt_increment;
				if (ol_supply_w_id == w_id) {
					s_remote_cnt_increment = 0;
				} else {
					s_remote_cnt_increment = 1;
				}
				// update stock row
				stmtUpdateStock = connect.prepareStatement("UPDATE " + "STOCK" + " SET S_QUANTITY = ?," + "S_YTD = ?,"
						+ "S_ORDER_CNT = ?," + "S_REMOTE_CNT = ? " + " WHERE S_I_ID = ? " + "   AND S_W_ID = ?");
				stmtUpdateStock.setDouble(1, s_quantity);
				stmtUpdateStock.setDouble(2, s_ytd + ol_quantity);
				stmtUpdateStock.setInt(3, s_order_cnt + 1);
				stmtUpdateStock.setInt(4, s_remote_cnt + s_remote_cnt_increment);
				stmtUpdateStock.setLong(5, ol_i_id);
				stmtUpdateStock.setLong(6, ol_supply_w_id);
				stmtUpdateStock.executeUpdate();
				// System.out.println("o_id: " + o_id + " s_ytd: " + s_ytd + " ol_quantity: " +
				// ol_quantity
				// + " ol_i_id: " + ol_i_id);
				//
				double ol_amount = ol_quantity * i_price;
				orderLineAmounts[ol_number - 1] = ol_amount;
				total_amount += ol_amount;
				if (i_data.indexOf("ORIGINAL") != -1 && s_data.indexOf("ORIGINAL") != -1) {
					brandGeneric[ol_number - 1] = 'B';
				} else {
					brandGeneric[ol_number - 1] = 'G';
				}
				String ol_dist_info = null;
				switch ((int) (long) d_id) {
				case 1:
					ol_dist_info = s_dist_01;
					break;
				case 2:
					ol_dist_info = s_dist_02;
					break;
				case 3:
					ol_dist_info = s_dist_03;
					break;
				case 4:
					ol_dist_info = s_dist_04;
					break;
				case 5:
					ol_dist_info = s_dist_05;
					break;
				case 6:
					ol_dist_info = s_dist_06;
					break;
				case 7:
					ol_dist_info = s_dist_07;
					break;
				case 8:
					ol_dist_info = s_dist_08;
					break;
				case 9:
					ol_dist_info = s_dist_09;
					break;
				case 10:
					ol_dist_info = s_dist_10;
					break;
				}

				//
				// insert a row into orderline table representing each order item
				PreparedStatement i_stmt = connect.prepareStatement("INSERT INTO " + "ORDER_LINE"
						+ " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) "
						+ " VALUES (?,?,?,?,?,?,?,?,?)");
				i_stmt.setInt(1, o_id);
				i_stmt.setLong(2, d_id);
				i_stmt.setLong(3, w_id);
				i_stmt.setInt(4, ol_number);
				i_stmt.setLong(5, ol_i_id);
				i_stmt.setLong(6, ol_supply_w_id);
				i_stmt.setDouble(7, ol_quantity);
				i_stmt.setDouble(8, ol_amount);
				i_stmt.setString(9, ol_dist_info);
				i_stmt.executeUpdate();

			}
			// System.out.println("=======");
			// i_stmt.executeBatch();
			// stmtUpdateStock.executeBatch();
			total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);
			// stmt.clearBatch();
			stmt.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	
	// ***********************************************************************************
		//
		//
		//
		//
		//
		//
		//
		//
		// ************************************************************************************
	public void payment(Long w_id, Long d_id, Long customerByName, Long c_id, String c_last,
			Long customerWarehouseID, Long customerDistrictID, Long paymentAmount) throws Exception {
		PreparedStatement stmt = null;
		try {
			boolean isRemote = (w_id != customerDistrictID);
			double w_ydt, d_ytd;
			String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
			String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;
			// read necessary columns from warehouse
			stmt = connect.prepareStatement("SELECT W_YTD, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME"
					+ "  FROM " + "WAREHOUSE" + " WHERE W_ID = ?");
			stmt.setLong(1, w_id);
			ResultSet w_rs = stmt.executeQuery();
			if (!w_rs.next()) {
				System.out.println("ERROR_21: Invalid warehouse id: " + w_id);
			}
			w_ydt = w_rs.getDouble("W_YTD");
			w_street_1 = w_rs.getString("W_STREET_1");
			w_street_2 = w_rs.getString("W_STREET_2");
			w_city = w_rs.getString("W_CITY");
			w_state = w_rs.getString("W_STATE");
			w_zip = w_rs.getString("W_ZIP");
			w_name = w_rs.getString("W_NAME");
			w_rs.close();
			//
			// update W_YTD by paymentAmount
			stmt = connect.prepareStatement("UPDATE " + "WAREHOUSE" + "   SET W_YTD = ? " + " WHERE W_ID = ? ");
			stmt.setDouble(1, w_ydt + paymentAmount);
			stmt.setLong(2, w_id);
			stmt.executeUpdate();

			//
			// read necessary columns from district
			stmt = connect.prepareStatement("SELECT D_YTD, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME"
					+ "  FROM " + "DISTRICT" + " WHERE D_W_ID = ? " + "   AND D_ID = ?");
			stmt.setLong(1, w_id);
			stmt.setLong(2, d_id);
			ResultSet d_rs = stmt.executeQuery();
			if (!d_rs.next()) {
				System.out.println("ERROR_22: Invalid district id: " + w_id + "," + d_id);
			}
			d_ytd = d_rs.getDouble("D_YTD");
			d_street_1 = d_rs.getString("D_STREET_1");
			d_street_2 = d_rs.getString("D_STREET_2");
			d_city = d_rs.getString("D_CITY");
			d_state = d_rs.getString("D_STATE");
			d_zip = d_rs.getString("D_ZIP");
			d_name = d_rs.getString("D_NAME");
			d_rs.close();
			//
			// update D_YTD by paymentAmount
			stmt = connect
					.prepareStatement("UPDATE " + "DISTRICT" + "   SET D_YTD = ? " + " WHERE D_W_ID = ? AND D_ID = ? ");
			stmt.setDouble(1, d_ytd + paymentAmount);
			stmt.setLong(2, w_id);
			stmt.setLong(3, d_id);
			stmt.executeUpdate();

			//
			// Retrieve customer's information

			String c_first, c_middle, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit;
			double c_credit_lim, c_discount, c_balance;
			float c_ytd_payment;
			int c_payment_cnt;
			Timestamp c_since;
			if (customerByName==0) {
				stmt = connect.prepareStatement("SELECT C_ID" + "  FROM " + "CUSTOMER" + " WHERE C_W_ID = ? "
						+ "   AND C_D_ID = ? " + "   AND C_LAST = ? " + "ALLOW FILTERING");
				stmt.setLong(1, customerWarehouseID);
				stmt.setLong(2, customerDistrictID);
				stmt.setString(3, c_last);
				ResultSet c_rs = stmt.executeQuery();
				// find the appropriate index
				int index = 0;
				List<Long> all_c_ids = new ArrayList<Long>();
				while (c_rs.next()) {
					index++;
					all_c_ids.add(c_rs.getLong("C_ID"));
				}
				if (index == 0) {
					System.out.println("ERROR_23: No customer with the given last name: " + customerWarehouseID + ","
							+ customerDistrictID + "," + c_last);
				}
				if (index % 2 != 0)
					index++;
				index = (index / 2);
				c_id = all_c_ids.get(index - 1);

				stmt = connect.prepareStatement("SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,"
						+ "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM,"
						+ "   C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " + "  FROM " + "CUSTOMER"
						+ " WHERE C_W_ID = ? " + "   AND C_D_ID = ? " + "   AND C_ID = ? ");
				stmt.setLong(1, customerWarehouseID);
				stmt.setLong(2, customerDistrictID);
				stmt.setLong(3, c_id);
				c_rs = stmt.executeQuery();

				c_first = c_rs.getString("c_first");
				c_middle = c_rs.getString("c_middle");
				c_street_1 = c_rs.getString("c_street_1");
				c_street_2 = c_rs.getString("c_street_2");
				c_city = c_rs.getString("c_city");
				c_state = c_rs.getString("c_state");
				c_zip = c_rs.getString("c_zip");
				c_phone = c_rs.getString("c_phone");
				c_credit = c_rs.getString("c_credit");
				c_credit_lim = c_rs.getDouble("c_credit_lim");
				c_discount = c_rs.getDouble("c_discount");
				c_balance = c_rs.getDouble("c_balance");
				c_ytd_payment = c_rs.getFloat("c_ytd_payment");
				c_payment_cnt = c_rs.getInt("c_payment_cnt");
				c_since = c_rs.getTimestamp("c_since");
				c_rs.close();

			} else {
				// retrieve customer by id
				stmt = connect.prepareStatement("SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, "
						+ "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
						+ "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " + "  FROM "
						+ "CUSTOMER" + " WHERE C_W_ID = ? " + "   AND C_D_ID = ? " + "   AND C_ID = ?");
				stmt.setLong(1, customerWarehouseID);
				stmt.setLong(2, customerDistrictID);
				stmt.setLong(3, c_id);
				ResultSet c_rs = stmt.executeQuery();
				if (!c_rs.next()) {
					System.out.println("ERROR_24: Invalid customer id: " + customerWarehouseID + ","
							+ customerDistrictID + "," + c_id);
				}
				c_first = c_rs.getString("c_first");
				c_middle = c_rs.getString("c_middle");
				c_street_1 = c_rs.getString("c_street_1");
				c_street_2 = c_rs.getString("c_street_2");
				c_city = c_rs.getString("c_city");
				c_state = c_rs.getString("c_state");
				c_zip = c_rs.getString("c_zip");
				c_phone = c_rs.getString("c_phone");
				c_credit = c_rs.getString("c_credit");
				c_credit_lim = c_rs.getDouble("c_credit_lim");
				c_discount = c_rs.getDouble("c_discount");
				c_balance = c_rs.getDouble("c_balance");
				c_ytd_payment = c_rs.getFloat("c_ytd_payment");
				c_payment_cnt = c_rs.getInt("c_payment_cnt");
				c_since = c_rs.getTimestamp("c_since");
				c_rs.close();
			}
			//
			// Update customers info
			c_balance -= paymentAmount;
			c_ytd_payment += paymentAmount;
			c_payment_cnt++;
			String c_data = null;
			if (c_credit.equals("BC")) {
				// bad credit (c_data is also updated)
				stmt = connect.prepareStatement("SELECT C_DATA " + "  FROM " + "CUSTOMER" + " WHERE C_W_ID = ? "
						+ "   AND C_D_ID = ? " + "   AND C_ID = ?");
				stmt.setLong(1, customerWarehouseID);
				stmt.setLong(2, customerDistrictID);
				stmt.setLong(3, c_id);
				ResultSet c_rs = stmt.executeQuery();
				if (!c_rs.next()) {
					System.out.println("ERROR_25: Invalid customer id: " + customerWarehouseID + ","
							+ customerDistrictID + "," + c_id);
				}
				c_data = c_rs.getString("C_DATA");
				c_rs.close();
				c_data = c_id + " " + customerDistrictID + " " + customerWarehouseID + " " + d_id + " " + w_id + " "
						+ paymentAmount + " | " + c_data;
				if (c_data.length() > 500)
					c_data = c_data.substring(0, 500);
				stmt = connect.prepareStatement("UPDATE " + "CUSTOMER" + "   SET C_BALANCE = ?, "
						+ "       C_YTD_PAYMENT = ?, " + "       C_PAYMENT_CNT = ?, " + "       C_DATA = ? "
						+ " WHERE C_W_ID = ? " + "   AND C_D_ID = ? " + "   AND C_ID = ?");
				stmt.setDouble(1, c_balance);
				stmt.setFloat(2, c_ytd_payment);
				stmt.setInt(3, c_payment_cnt);
				stmt.setString(4, c_data);
				stmt.setLong(5, customerWarehouseID);
				stmt.setLong(6, customerDistrictID);
				stmt.setLong(7, c_id);
				stmt.executeUpdate();
			} else {
				// good credit (no need to update c_data)
				stmt = connect.prepareStatement("UPDATE " + "CUSTOMER" + "   SET C_BALANCE = ?, "
						+ "       C_YTD_PAYMENT = ?, " + "       C_PAYMENT_CNT = ? " + " WHERE C_W_ID = ? "
						+ "   AND C_D_ID = ? " + "   AND C_ID = ?");
				stmt.setDouble(1, c_balance);
				stmt.setDouble(2, c_ytd_payment);
				stmt.setInt(3, c_payment_cnt);
				stmt.setLong(4, customerWarehouseID);
				stmt.setLong(5, customerDistrictID);
				stmt.setLong(6, c_id);
				stmt.executeUpdate();
			}

			// create H_DATA and insert a new row into HISTORY
			if (w_name.length() > 10)
				w_name = w_name.substring(0, 10);
			if (d_name.length() > 10)
				d_name = d_name.substring(0, 10);
			String h_data = w_name + "    " + d_name;
			stmt = connect.prepareStatement("SELECT H_AMOUNT FROM HISTORY WHERE" + " H_C_D_ID=?" + " AND H_C_W_ID=?"
					+ " AND H_C_ID=?" + " AND H_D_ID=?" + " AND H_W_ID=?");
			stmt.setLong(1, customerDistrictID);
			stmt.setLong(2, customerWarehouseID);
			stmt.setLong(3, c_id);
			stmt.setLong(4, d_id);
			stmt.setLong(5, w_id);
			ResultSet h_rs = stmt.executeQuery();
			double old_amount = 0;
			if (h_rs.next())
				old_amount += h_rs.getDouble("H_AMOUNT");
			stmt = connect.prepareStatement("INSERT INTO " + "HISTORY"
					+ " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) "
					+ " VALUES (?,?,?,?,?,?,?,?)");
			stmt.setLong(1, customerDistrictID);
			stmt.setLong(2, customerWarehouseID);
			stmt.setLong(3, c_id);
			stmt.setLong(4, d_id);
			stmt.setLong(5, w_id);
			stmt.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
			stmt.setDouble(7, old_amount + paymentAmount);
			stmt.setString(8, h_data);
			stmt.executeUpdate();

			//
			//
			//
			//
			//
			//
			//
			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			// TXN SUCCESSFUL!
			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
