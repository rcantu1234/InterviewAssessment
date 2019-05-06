package com.luv2code.springboot.interviewassessment.controller;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Scanner;

import com.mysql.cj.xdevapi.Statement;
import com.opencsv.CSVReader;

public class ImportCsv {

	public static void main(String[] args) {
		readCsv();
	}
	
	private static void readCsv()
    {
		String jdbcURL = "jdbc:mysql://localhost:3306/PERSON?userSSL=false&serverTimezone=UTC";
		String userName = "root";
		String password = "password";
		String driver = "com.mysql.cj.jdbc.Driver";
 
        try (@SuppressWarnings("deprecation")
		CSVReader reader = new CSVReader(new FileReader("persons.csv"), ','); )
                    
        {
			System.out.println("Connecting to database : " + jdbcURL);
			Class.forName(driver);
			
			Connection conn = DriverManager.getConnection(jdbcURL, userName, password);
			
			System.out.println("Connection Successful!!!");
			String insertQuery = "Insert into X (A, B, C, D, E, F, G, H, I, J) values (?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement pstmt = conn.prepareStatement(insertQuery);
            String[] rowData = null;
            int i = 0;
            
            readLines();

            while((rowData = reader.readNext()) != null)
            {
            	
                for (String data : rowData)
                {
                    pstmt.setString((i % 10) + 1, data);
 
                    if (++i % 10 == 0)
                       pstmt.addBatch();// add batch
                    System.out.println(data);
 
                    if (i % 30 == 0)// insert when the batch size is 10
                    pstmt.executeBatch();
                }
        }		
        System.out.println("Data Successfully Uploaded");
        }
        catch (Exception e)
        {
                e.printStackTrace();
        }
 
    }
	
	public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((Character.isWhitespace(str.charAt(i)) == false)) {
                return false;
            }
        }
        return true;
    }
	
	public static void readLines() {
        Scanner s = null;
        try {
            s = new Scanner(new File("persons.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        
        while (s.hasNextLine()) {
            String[] line = s.nextLine().split(",");
            for (String element : line) {
                if (!isBlank(element))
                    System.out.println(element);
            }

        }
	}
}
