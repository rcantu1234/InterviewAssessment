package com.luv2code.springboot.interviewassessment.controller;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.mysql.cj.jdbc.Blob;
import com.opencsv.CSVReader;

public class TestDB {

	public static void main(String[] args) throws IOException {
		String jdbcURL = "jdbc:mysql://localhost:3306/PERSON?userSSL=false&serverTimezone=UTC";
		String userName = "root";
		String password = "password";
		String driver = "com.mysql.cj.jdbc.Driver";
		String[] firstNames = null;
		String[] lastNames = null;
		String[] emails = null;
		String[] ages = null;
		Blob blob = null;
		

		try {
			
			System.out.println("Connecting to database : " + jdbcURL);
			Class.forName(driver);
			
			Connection conn = DriverManager.getConnection(jdbcURL, userName, password);
			
			System.out.println("Connection Successful!!!");
						
		        BufferedReader bReader = new BufferedReader(new FileReader("persons.csv"));
		        String line = ""; 
		        while ((line = bReader.readLine()) != null) {
		            try {

		                if (line != null) 
		                {
		                    String[] array = line.split(",+");
		                    for(String result:array)
		                    {
		                        System.out.println(result);
		 //Create preparedStatement here and set them and excute them
		                		String sql = " INSERT INTO person(first_name,last_name,email,age, image) VALUES(?,?,?,?,?) ";

		                PreparedStatement ps = (PreparedStatement) conn.prepareStatement(sql);
		                
		                // Add rows to a batch in a loop. Each iteration adds a
		                // new row.
		                for (int i = 0; i < firstNames.length; i++) {
		                    // Add each parameter to the row.
		                     ps.setInt(1, i + 1);
			                 ps.setString(2,firstNames[i]);
			                 ps.setString(3,lastNames[i]);
			                 ps.setString(4,emails[i]);
			                 ps.setString(5,ages[i]);
			                 ps.setBlob(5,blob);
			                 ps.execute();
			                 ps. close();
		                    // Add row to the batch.
		                    ps.addBatch();
		                }
//		                 ps.setString(1,firstNames[0]);
//		                 ps.setString(2,lastNames[1]);
//		                 ps.setString(3,emails[2]);
//		                 ps.setString(4,ages[3]);
//		                 ps.setBlob(5,blob);
//		                 ps.execute();
//		                 ps. close();
		   //Assuming that your line from file after split will folllow that sequence

		                    }
		                } 
		            }
		            finally
		            {
		               if (bReader == null) 
		                {
		                    bReader.close();
		                }
		            }
		        }
				conn.close();
		    } catch (FileNotFoundException ex) {
		        ex.printStackTrace();
		    } catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	}

}
