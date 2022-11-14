package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }
    
    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month
            
            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);
            
            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }
    
    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */

            FirstNameInfo info = new FirstNameInfo();

            ResultSet rst = stmt.executeQuery(
                "SELECT DISTINCT FIRST_NAME, LENGTH(FIRST_NAME) AS LengthOfName " +
                "FROM " + UsersTable + " " +  
                "ORDER BY LengthOfName DESC, FIRST_NAME");

            int longest = -1;
            while (rst.next()) {
                // System.out.println("longest length " + longest);
                if (rst.isFirst()) {
                    longest = rst.getInt(2);
                }
                if (longest == rst.getInt(2)) {
                    // System.out.println("Name " + rst.getString(1));
                    info.addLongName(rst.getString(1));
                } else {
                    break;
                }
            }

            rst = stmt.executeQuery(
                "SELECT DISTINCT FIRST_NAME, LENGTH(FIRST_NAME) AS LengthOfName " +
                "FROM " + UsersTable + " " +  
                "ORDER BY LengthOfName ASC, FIRST_NAME");
            
            longest = -1;
            while (rst.next()) {
                if (rst.isFirst()) {
                    longest = rst.getInt(2);
                }
                if (longest == rst.getInt(2)) {
                    info.addShortName(rst.getString(1));
                } else {
                    break;
                }
            }

            // most common name
            rst = stmt.executeQuery(
                "SELECT FIRST_NAME, COUNT(FIRST_NAME) AS most_frequent " +
                "FROM " + UsersTable + " " +
                "GROUP BY FIRST_NAME " +
                "ORDER BY most_frequent DESC, FIRST_NAME DESC");

            
            int common = -1;
            while (rst.next()) {
                if (rst.isFirst()) {
                    common = rst.getInt(2);
                    info.setCommonNameCount(common);
                }
                if (rst.getInt(2) == common) {
                    info.addCommonName(rst.getString(1));
                } else if (rst.getInt(2) < common) {
                    break;
                }
            }

            rst.close();
            stmt.close();

            return info;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }
    
    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst = stmt.executeQuery(
                //        1         2           3
                "SELECT User_ID, First_Name, Last_Name " +                                      // Find the IDs, first names, and last names
                "FROM " + UsersTable + " " +                                                    // from all users
                "WHERE User_ID NOT IN (SELECT user1_id FROM " + FriendsTable + " " +            // without any friends
                "UNION SELECT user2_id FROM " + FriendsTable + ")");
            
            while (rst.next()) {
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                results.add(u1);
            }

            rst.close();
            stmt.close();
            return results;

            
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return results;
        }
    }
    
    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */


            ResultSet rst = stmt.executeQuery(
                "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                "FROM " + UsersTable + " U " +
                "INNER JOIN " + CurrentCitiesTable + " C ON C.USER_ID = U.USER_ID " +
                "INNER JOIN " + HometownCitiesTable + " H ON H.USER_ID = U.USER_ID " +
                "WHERE (C.CURRENT_CITY_ID is not NULL " +
                "AND H.HOMETOWN_CITY_ID is not NULL " +
                "AND C.CURRENT_CITY_ID != H.HOMETOWN_CITY_ID) " +
                "ORDER BY U.USER_ID");
            
            while (rst.next()) { 
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                results.add(u1);
            }

            rst.close();
            stmt.close();
            return results;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return results;
        }
    }
    
    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */

            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW top_n AS " +
                "SELECT PHOTO_ID FROM ( " +
                "SELECT p.photo_id, COUNT(*) " +
                "FROM " + PhotosTable + " P left join " + TagsTable + " T on P.photo_id = T.tag_photo_id " +
                "GROUP BY p.photo_id ORDER BY 2 DESC, 1 ASC) " +
                "WHERE ROWNUM <= " + num
            );
         
            ResultSet rst = stmt.executeQuery(
                //        1                 2            3            4                5            6            7           8
                "SELECT top_n.photo_id, P.album_id, A.album_name, P.photo_caption, P.photo_link, U.user_id, U.first_name, U.last_name " + 
                "FROM top_n, " + PhotosTable + " P, " +  AlbumsTable + " A, " + TagsTable + " T, " + UsersTable + " U " +
                "WHERE top_n.photo_id = P.photo_id AND P.album_id = A.album_id AND top_n.photo_id = T.tag_photo_id AND T.tag_subject_id = U.user_id " +
                "ORDER BY 1, 6");
            
            // temp
            PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
            TaggedPhotoInfo tp = new TaggedPhotoInfo(p);

            Long photoid = (long)-1;
            boolean first = true;
            while (rst.next()) {
                if (rst.getLong(1) != photoid) {
                    if (first) {
                        first = false;
                    } else {
                        results.add(tp);
                    }
                    p = new PhotoInfo(rst.getLong(1), rst.getLong(2), rst.getString(5), rst.getString(3));
                    tp = new TaggedPhotoInfo(p);
                    photoid = rst.getLong(1);
                }
                UserInfo u1 = new UserInfo(rst.getLong(6), rst.getString(7), rst.getString(8));
                tp.addTaggedUser(u1);

                if (rst.isLast()) {
                    results.add(tp);
                }
            }

            stmt.executeUpdate(
                "DROP VIEW top_n"
            );
            
            rst.close();
            stmt.close();
            return results;

            
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return results;
        }
        
    }
    
    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW temp AS " +
                "SELECT U1.USER_ID AS U1ID, U1.FIRST_NAME AS U1FN, U1.LAST_NAME AS U1LN, U1.YEAR_OF_BIRTH AS U1Y, U2.USER_ID AS U2ID, U2.FIRST_NAME AS U2FN, " + 
                "U2.LAST_NAME AS U2LN, U2.YEAR_OF_BIRTH AS U2Y, P.PHOTO_ID AS PID, P.PHOTO_LINK As PLINK, A.ALBUM_ID AS AID, A.ALBUM_NAME AS ANAME " +
                "FROM " + TagsTable + " T1 " +
                "JOIN " + TagsTable + " T2 ON T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID and T1.TAG_SUBJECT_ID < T2.TAG_SUBJECT_ID " +
                "JOIN " + PhotosTable + " P ON P.PHOTO_ID = T1.TAG_PHOTO_ID " +
                "JOIN " + AlbumsTable + " A ON A.ALBUM_ID = P.ALBUM_ID " +
                "JOIN " + UsersTable + " U1 ON U1.USER_ID = T1.TAG_SUBJECT_ID " +
                "JOIN " + UsersTable + " U2 ON U2.USER_ID = T2.TAG_SUBJECT_ID " +
                "WHERE U1.USER_ID != U2.USER_ID " +
                "AND U1.GENDER = U2.GENDER " +
                "AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= " + yearDiff +
                "AND (T1.TAG_SUBJECT_ID, T2.TAG_SUBJECT_ID) NOT IN (SELECT * FROM Project2.PUBLIC_Friends)");

            ResultSet rst = stmt.executeQuery(
                "SELECT U1ID, U2ID " +
                "FROM " +
                "(SELECT U1ID, U2ID, COUNT(*) " +
                "FROM temp " +
                "GROUP BY U1ID, U2ID " +
                "ORDER BY COUNT(*) DESC, U1ID ASC, U2ID ASC) " +
                "WHERE ROWNUM <= 1");
            
            while (rst.next()) {
                long U1ID = rst.getLong(1);
                long U2ID = rst.getLong(2);
                //System.out.println("HERE");

                try (Statement temp2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
                    ResultSet rst2 = temp2.executeQuery(
                        //       1     2     3     4     5     6     7     8     9    10
                        "SELECT U1FN, U1LN, U2FN, U2LN, PID, PLINK, AID, ANAME, U1Y, U2Y " +
                        "FROM temp " +
                        "WHERE U1ID = " + U1ID + " AND U2ID = " + U2ID + " " +
                        "ORDER BY PID");
                    
                    // temp
                    UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                    UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                    MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                    
                    while (rst2.next()) {
                        //System.out.println("I'm HERE");
                        if (rst2.isFirst()) {
                            u1 = new UserInfo(U1ID, rst2.getString(1), rst2.getString(2));
                            u2 = new UserInfo(U2ID, rst2.getString(3), rst2.getString(4));
                            mp = new MatchPair(u1, rst2.getLong(9), u2, rst2.getLong(10));
                        }
                        PhotoInfo p = new PhotoInfo(rst2.getLong(5), rst2.getLong(7), rst2.getString(6), rst2.getString(8));
                        mp.addSharedPhoto(p);

                        if (rst2.isLast()) {
                            results.add(mp);
                        }
                    }
                    rst2.close();
                    temp2.close();
                }
                catch (SQLException e) {
                    System.err.println(e.getMessage());
                }
            }

            stmt.executeUpdate(
                "DROP VIEW temp"
            );

            rst.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }
    
    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW friends_2 AS " +
                "SELECT F1.user1_id, F1.user2_id FROM " + FriendsTable + " F1 " +
                "UNION SELECT F2.user2_id, F2.user1_id FROM " + FriendsTable + " F2");

            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW friends_common AS " +
                "SELECT F1.user1_id as u1_id, F2.user1_id as u2_id, F1.user2_id as u3_id " +
                "FROM friends_2 F1, friends_2 F2 " +
                "WHERE F1.user1_id < F2.user1_id AND F1.user2_id = F2.user2_id");

            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW top_n2 AS " +
                "SELECT u1_id, u2_id " +
                "FROM (SELECT u1_id, u2_id, count(u3_id) " +
                "FROM friends_common " +
                "WHERE NOT EXISTS (SELECT * FROM " + FriendsTable + " " +
                "F WHERE F.user1_id = u1_id AND F.user2_id = u2_id) " +
                "GROUP BY u1_id, u2_id " +
                "ORDER BY 3 desc, 1 asc, 2 asc) " +
                "WHERE rownum <= " + num);
      
            ResultSet rst = stmt.executeQuery(
                //         1        2        3        4               5              6            7              8             9
                "SELECT T.u1_id, T.u2_id, F.u3_id, U1.first_name, U1.last_name, U2.first_name, U2.last_name, U3.first_name, U3.last_name " +
                "FROM top_n2 T, friends_common F, " + 
                UsersTable + " U1, " + 
                UsersTable + " U2, " + 
                UsersTable + " U3 " +
                "WHERE T.u1_id = F.u1_id AND T.u2_id = F.u2_id AND T.u1_id = U1.user_id AND T.u2_id = U2.user_id AND F.u3_id = U3.user_id " +
                "ORDER BY 1 ASC, 2 ASC");

            long id1 = -1;
            long id2 = -1;

            UserInfo u1 = new UserInfo(16, "The", "Hacker");
            UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
            UsersPair up = new UsersPair(u1, u2);

            while (rst.next()) {
                if (rst.getLong(1) != id1 && rst.getLong(2) != id2) {
                    if (id1 != -1) {
                        results.add(up);
                    }
                    id1 = rst.getLong(1);
                    id2 = rst.getLong(2);
                    u1 = new UserInfo(id1, rst.getString(4), rst.getString(5));
                    u2 = new UserInfo(id2, rst.getString(6), rst.getString(7));
                    up = new UsersPair(u1, u2);
                }
                UserInfo u3 = new UserInfo(rst.getLong(3), rst.getString(8), rst.getString(9));
                up.addSharedFriend(u3);

                if (rst.isLast()) {
                    results.add(up);
                }
            }

            rst.close();
            stmt.close();
            return results;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return results;
        }
    }
    
    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT DISTINCT C.STATE_NAME, COUNT(C.STATE_NAME) AS frequency " +
                "FROM " + EventsTable + " E "+
                "LEFT JOIN " + CitiesTable + " C ON E.EVENT_CITY_ID = C.CITY_ID " +
                "WHERE C.STATE_NAME IS NOT NULL " + 
                "GROUP BY C.STATE_NAME " +
                "ORDER BY frequency DESC, C.STATE_NAME ASC");

            // System.out.println("HERE");
            
            int num = -1;
            EventStateInfo info = new EventStateInfo(-1);
            while (rst.next()) {
                if (rst.isFirst()) {
                    num = rst.getInt(2);
                    info = new EventStateInfo(num);
                }
                if (rst.getInt(2) == num) {
                    info.addState(rst.getString(1));
                } else if (rst.getInt(2) < num) {
                    break;
                }
            }
            
            rst.close();
            stmt.close();
            // System.out.println("2222");
            return info;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }
    
    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT user_id, first_name, last_name " +
                "FROM " + UsersTable + " " +
                "WHERE user_id in (SELECT F1.user1_id FROM " + FriendsTable + " F1 WHERE F1.user2_id = " + userID + " " +
                "UNION SELECT F2.user2_id FROM " + FriendsTable + " F2 WHERE F2.user1_id = " + userID + ") " +
                "ORDER BY year_of_birth ASC, month_of_birth ASC, day_of_birth ASC, user_id DESC");

            UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
            UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");

            while (rst.next()) {
                if (rst.isFirst()) {
                    old = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                } else {
                    break;
                }
            }

            rst = stmt.executeQuery(
                "SELECT user_id, first_name, last_name " +
                "FROM " + UsersTable + " " +
                "WHERE user_id in (SELECT F1.user1_id FROM " + FriendsTable + " F1 WHERE F1.user2_id = " + userID + " " +
                "UNION SELECT F2.user2_id FROM " + FriendsTable + " F2 WHERE F2.user1_id = " + userID + ") " +
                "ORDER BY year_of_birth DESC, month_of_birth DESC, day_of_birth DESC, user_id DESC");

            while (rst.next()) {
                if (rst.isFirst()) {
                    young = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                } else {
                    break;
                }
            }

            rst.close();
            stmt.close();
            return new AgeInfo(old, young); 
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }
    
    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */

            ResultSet rst = stmt.executeQuery(
                //          1             2            3             4           5            6
                "SELECT U1.user_id, U1.first_name, U1.last_name, U2.user_id, U2.first_name, U2.last_name " +
                "FROM " + UsersTable + " U1, " + UsersTable + " U2, " + HometownCitiesTable + " H1, " + HometownCitiesTable + " H2 " +
                "WHERE U1.user_id < U2.user_id AND U1.last_name = U2.last_name AND U1.user_id = H1.user_id and U2.user_id = H2.user_id AND H1.hometown_city_id = H2.hometown_city_id " +
                "AND EXISTS(SELECT * FROM " + FriendsTable + " F WHERE (F.user1_id = U1.user_id and F.user2_id = U2.user_id) OR (F.user1_id = U2.user_id and F.user2_id = U1.user_id)) " +
                "AND U1.year_of_birth - U2.year_of_birth >= -10 AND U2.year_of_birth - U1.year_of_birth >= -10 " +
                "ORDER BY U1.user_id ASC, U2.user_id ASC");
            

            while (rst.next()) {
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getLong(4), rst.getString(5), rst.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }

            rst.close();
            stmt.close();
            return results;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return results;
        }
    }
    
    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
