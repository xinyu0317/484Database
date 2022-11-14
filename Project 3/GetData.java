import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Vector;



//json.simple 1.1
// import org.json.simple.JSONObject;
// import org.json.simple.JSONArray;

// Alternate implementation of JSON modules.
import org.json.JSONObject;
import org.json.JSONArray;

public class GetData{

    static String prefix = "project3.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding 
    // tables in your database

    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;

    // This is the data structure to store all users' information
    // DO NOT change the name
    JSONArray users_info = new JSONArray();		// declare a new JSONArray


    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
	super();
	String dataType = u;
	oracleConnection = c;
	// You will use the following tables in your Java code
	cityTableName = prefix+dataType+"_CITIES";
	userTableName = prefix+dataType+"_USERS";
	friendsTableName = prefix+dataType+"_FRIENDS";
	currentCityTableName = prefix+dataType+"_USER_CURRENT_CITIES";
	hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITIES";
	programTableName = prefix+dataType+"_PROGRAMS";
	educationTableName = prefix+dataType+"_EDUCATION";
	eventTableName = prefix+dataType+"_USER_EVENTS";
	albumTableName = prefix+dataType+"_ALBUMS";
	photoTableName = prefix+dataType+"_PHOTOS";
	tagTableName = prefix+dataType+"_TAGS";
    }




    //implement this function

    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException{

    	JSONArray users_info = new JSONArray();

	// Your implementation goes here....


    try (Statement statements = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY)) {

    	ResultSet result = statements.executeQuery(
			"select user_id, first_name, last_name, gender, year_of_birth, month_of_birth, day_of_birth from " + userTableName);
			int i = 0;

    	while (result.next()) {
    		JSONObject user = new JSONObject();
			int uid = result.getInt(1);
    		user.put("user_id", uid);
    		user.put("first_name", result.getString(2));
    		user.put("last_name", result.getString(3));
    		user.put("gender", result.getString(4));
    		user.put("YOB", result.getInt(5));
    		user.put("MOB", result.getInt(6));
    		user.put("DOB", result.getInt(7));

		

		Statement statements_1 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
		//Hometown
    	ResultSet result_1 = statements_1.executeQuery(
			"SELECT City.city_name, City.state_name, City.country_name FROM " +
				hometownCityTableName + " Home " + 
				"JOIN " + cityTableName + " City " + 
    			"ON Home.hometown_city_id = City.city_id " + 
				"WHERE Home.user_id = " + uid
			);

    	result_1.next();
    	JSONObject hometown = new JSONObject();
    	hometown.put("city", result_1.getString(1));
    	hometown.put("state", result_1.getString(2));
    	hometown.put("country", result_1.getString(3));
		user.put("hometown", hometown);
    

		//Current city
		result_1 = statements_1.executeQuery(
			"select City.city_name, City.state_name, City.country_name from " +
				currentCityTableName + " Curr " + 
				"join " + cityTableName + " City " +
				"on Curr.current_city_id = City.city_id " + 
				"where Curr.user_id = " + uid 
			);
		
		result_1.next();
		JSONObject current = new JSONObject();
		current.put("city", result_1.getString(1));
		current.put("state", result_1.getString(2));
		current.put("country", result_1.getString(3));
		user.put("current", current);
	

		//List of Friends
    	result_1 = statements_1.executeQuery(
			"select user2_id from " + friendsTableName +
			" where user1_id = " + uid 
		);
    	
		JSONArray friends = new JSONArray();
		while (result_1.next()){
			friends.put(result_1.getInt(1));
		}

		statements_1.close();
		user.put("friends", friends);
		users_info.put(user);
		}

    	result.close();
        statements.close();
    } 
	/*
	catch (SQLException sqe) {
        System.sqe.println(sqe.getMessage());
    }
	*/
		return users_info;
    }


    // This outputs to a file "output.json"
    public void writeJSON(JSONArray users_info) {
	// DO NOT MODIFY this function
	try {
	    FileWriter file = new FileWriter(System.getProperty("user.dir")+"/output.json");
	    file.write(users_info.toString());
	    file.flush();
	    file.close();

	} catch (IOException e) {
	    e.printStackTrace();
	}
		
    }
}

