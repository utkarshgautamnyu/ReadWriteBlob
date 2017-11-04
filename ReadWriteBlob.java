import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class ReadWriteBlob {

	public static void writeBlob(String[] args) throws Exception {

		Connection connection = null;
		PreparedStatement statement = null;

		FileInputStream input = null;
		
		try {
			// Get a connection to database
			connection = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/table");

			// Prepare statement
			String sql = "update table set objectData=? where id='uniqueID'";
			statement = connection.prepareStatement(sql);
			
			// Set parameter for input file name
			File file = new File("File.pdf");
			input = new FileInputStream(file);
			statement.setBinaryStream(1, input);
			
			// Execute statement
			System.out.println(sql);
			statement.executeUpdate();
			
		} catch (Exception exc) {
			exc.printStackTrace();
		} finally {			
			if (input != null) {
				input.close();
			}
			
			close(connection, statement);			
		}
	}
	
	public static void readBlob(String[] args) throws Exception {

		Connection connection = null;
		Statement statement = null;
		ResultSet result = null;

		InputStream input = null;
		FileOutputStream output = null;

		try {
			// Get a connection to database
			connection = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/table");

			// Execute statement
			statement = connection.createStatement();
			String sql = "select objectData from table where id='uniqueID'";
			result = statement.executeQuery(sql);
			
			// Set up a handle to the file
			File file = new File("File.pdf");
			output = new FileOutputStream(file);

			if (result.next()) {

				input = result.getBinaryStream("objectData"); 
				System.out.println(sql);
				
				byte[] buffer = new byte[1024];
				while (input.read(buffer) > 0) {
					output.write(buffer);
				}
				);				
			}

		} catch (Exception exc) {
			exc.printStackTrace();
		} finally {
			if (input != null) {
				input.close();
			}

			if (output != null) {
				output.close();
			}
			
			close(connection, statement);
		}
	}

	private static void close(Connection connection, Statement statement)
			throws SQLException {

		if (statement != null) {
			statement.close();
		}
		
		if (connection != null) {
			connection.close();
		}
	}

}
