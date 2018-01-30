import sun.rmi.runtime.Log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.UUID;

public class Database {
    private LogEngine logEngine;

    private Connection connection;
    private String driverName = "jdbc:hsqldb:";
    private String username = "sa";
    private String password = "";

    private static Database instance = new Database();
    public Port port;

    private Database() {
        port = new Port();
    }

    public static Database getInstance() {
        return instance;
    }

    public class Port implements IDatabase {
        public void setLogEngine(LogEngine logEngine) {
            innerSetLogEngine(logEngine);
        }

        public void setup(String dataPath,boolean seats,boolean passengers,boolean tickets,
                          boolean baggage,boolean baggageTags) {
            innerSetup(dataPath,seats,passengers,tickets,baggage,baggageTags);
        }

        public void resetSeats(String dataPath) {
            innerResetSeats(dataPath);
        }

        public void resetTickets(String dataPath) {
            innerResetTickets(dataPath);
        }

        public void importCSVDataSeats(String dataPath) {
            innerImportCSVDataSeats(dataPath);
        }

        public Seat selectSeat(String dataPath,TicketClass ticketClass) {
            return innerSelectSeat(dataPath,ticketClass);
        }

        public void update(String dataPath,Seat seat) {
            innerUpdate(dataPath,seat);
        }

        public void importCSVDataTickets(String dataPath) {
            innerImportCSVDataTickets(dataPath);
        }

        public void update(String dataPath,Ticket ticket) {
            innerUpdate(dataPath,ticket);
        }

        public ArrayList<Ticket> selectTickets(String dataPath) {
            return innerSelectTickets(dataPath);
        }

        public Ticket selectTicket(String dataPath,Passenger passenger) {
            return innerSelectTicket(dataPath,passenger);
        }

        public void importCSVDataPassengers(String dataPath) {
            innerImportCSVDataPassengers(dataPath);
        }

        public void update(String dataPath,Passenger passenger) {
            innerUpdate(dataPath,passenger);
        }

        public ArrayList<Passenger> selectPassengers(String dataPath) {
            return innerSelectPassengers(dataPath);
        }

        public ArrayList<Passenger> selectPassengers(String dataPath,TicketClass ticketClass) {
            return innerSelectPassengers(dataPath,ticketClass);
        }

        public Passenger selectPassenger(String dataPath,String uuid) {
            return innerSelectPassenger(dataPath,uuid);
        }

        public void importCSVDataBaggage(String dataPath) {
            innerImportCSVDataBaggage(dataPath);
        }

        public ArrayList<Baggage> selectBaggageList(String dataPath) {
            return innerSelectBaggageList(dataPath);
        }

        public ArrayList<Baggage> selectBaggageList(String dataPath,Passenger passenger) {
            return innerSelectBaggageList(dataPath,passenger);
        }

        public void update(String dataPath,Baggage baggage) {
            innerUpdate(dataPath,baggage);
        }

        public void insert(String dataPath,BaggageTag baggageTag) {
            innerInsert(dataPath,baggageTag);
        }

        public void update(String dataPath,BaggageTag baggageTag) {
            innerUpdate(dataPath,baggageTag);
        }

        public BaggageTag selectBaggageTag(String dataPath,Baggage baggage) {
            return innerSelectBaggageTag(dataPath,baggage);
        }
    }

    public void innerSetLogEngine(LogEngine logEngine) {
        this.logEngine = logEngine;
    }

    public void startup(String dataPath) {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            String databaseURL = driverName + dataPath + "records.db";
            connection = DriverManager.getConnection(databaseURL,username,password);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public synchronized void update(String sqlStatement) {
        try {
            Statement statement = connection.createStatement();
            int result = statement.executeUpdate(sqlStatement);
            if (result == -1)
                System.out.println("error executing " + sqlStatement);
            statement.close();
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        }
    }

    public synchronized ResultSet select(String sqlStatement) {
        ResultSet resultSet = null;

        try {
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(sqlStatement);
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        }

        return resultSet;
    }

    // reset

    public void innerResetSeats(String dataPath) {
        startup(dataPath);
        String sqlStatement = "UPDATE seats SET available = 'y'";
        logEngine.write("Database","innerResetSeats","dataPath = " + dataPath,sqlStatement);
        update(sqlStatement);
        shutdown();
    }

    public void innerResetTickets(String dataPath) {
        startup(dataPath);
        String sqlStatement = "UPDATE tickets SET gate = '', seat = ''";
        logEngine.write("Database","innerResetTickets","dataPath = " + dataPath,sqlStatement);
        update(sqlStatement);
        shutdown();
    }

    // seats

    public void dropTableSeats() {
        String sqlStatement = "DROP TABLE seats IF EXISTS";
        logEngine.write("Database","dropTableSeats","-",sqlStatement);
        update(sqlStatement);
    }

    public void createTableSeats() {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("CREATE TABLE seats").append(" ( ");
        sqlStringBuilder.append("uuid VARCHAR(36) NOT NULL").append(",");
        sqlStringBuilder.append("ticket_class VARCHAR(8) NOT NULL").append(",");
        sqlStringBuilder.append("id VARCHAR(3) NOT NULL").append(",");
        sqlStringBuilder.append("available VARCHAR(1) NOT NULL").append(",");
        sqlStringBuilder.append("PRIMARY KEY (uuid)");
        sqlStringBuilder.append(" )");
        logEngine.write("Database","createTableSeats","-",sqlStringBuilder.toString());
        update(sqlStringBuilder.toString());
    }

    public String buildInsertSQLStatement(Seat seat) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("INSERT INTO seats (uuid,ticket_class,id,available) VALUES (");
        sqlStringBuilder.append("'").append(seat.getUUID()).append("'").append(",");
        sqlStringBuilder.append("'").append(seat.getTicketClass()).append("'").append(",");
        sqlStringBuilder.append("'").append(seat.getID()).append("'").append(",");
        sqlStringBuilder.append("'").append(seat.getAvailable()).append("'");
        sqlStringBuilder.append(")");
        return sqlStringBuilder.toString();
    }

    public void insert(Seat seat) {
        logEngine.write("Database","insert","seat = " + seat.getUUID(),buildInsertSQLStatement(seat));
        update(buildInsertSQLStatement(seat));
    }

    public String buildUpdateSQLStatement(Seat seat) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("UPDATE seats SET ticket_class = '").append(seat.getTicketClass()).append("'").append(",");
        sqlStringBuilder.append("id = '").append(seat.getID()).append("'").append(",");
        sqlStringBuilder.append("available = '").append(seat.getAvailable()).append("' ");
        sqlStringBuilder.append("WHERE uuid = '").append(seat.getUUID()).append("'");
        System.out.println(sqlStringBuilder.toString());
        return sqlStringBuilder.toString();
    }

    public void innerUpdate(String dataPath,Seat seat) {
        startup(dataPath);
        logEngine.write("Database","innerUpdate","seat = " + seat.getUUID(),buildUpdateSQLStatement(seat));
        update(buildUpdateSQLStatement(seat));
        shutdown();
    }

    public String buildSelectSQLStatement(TicketClass ticketClass) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("SELECT uuid,ticket_class,id,available FROM seats ");
        sqlStringBuilder.append("WHERE ticket_class = '").append(ticketClass).append("' ");
        sqlStringBuilder.append("AND available = 'y' ").append("LIMIT 1");
        return sqlStringBuilder.toString();
    }

    public Seat innerSelectSeat(String dataPath,TicketClass ticketClass) {
        Seat seat = null;

        try {
            startup(dataPath);

            logEngine.write("Database","innerSelectSeat","ticketClass = " + ticketClass,buildSelectSQLStatement(ticketClass));
            ResultSet resultSet = select(buildSelectSQLStatement(ticketClass));
            while (resultSet.next())
                seat = new Seat(resultSet.getString(1),TicketClass.valueOf(resultSet.getString(2)),resultSet.getString(3),resultSet.getString(4));

            logEngine.write("Database","innerSelectSeat","-","seat: " + seat.getUUID() + ", " + seat.getTicketClass() + ", " + seat.getAvailable());

            shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return seat;
    }

    // --- tickets

    public void dropTableTickets() {
        String sqlStatement = "DROP TABLE tickets IF EXISTS";
        logEngine.write("Database","dropTableTickets","-",sqlStatement);
        update(sqlStatement);
    }

    public void createTableTickets() {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("CREATE TABLE tickets").append(" ( ");
        sqlStringBuilder.append("uuid VARCHAR(36) NOT NULL").append(",");
        sqlStringBuilder.append("date VARCHAR(10) NOT NULL").append(",");
        sqlStringBuilder.append("time VARCHAR(5) NOT NULL").append(",");
        sqlStringBuilder.append("source VARCHAR(3) NOT NULL").append(",");
        sqlStringBuilder.append("destination VARCHAR(3) NOT NULL").append(",");
        sqlStringBuilder.append("flight VARCHAR(5) NOT NULL").append(",");
        sqlStringBuilder.append("ticket_class VARCHAR(8) NOT NULL").append(",");
        sqlStringBuilder.append("gate VARCHAR(3)").append(",");
        sqlStringBuilder.append("seat VARCHAR(3)").append(",");
        sqlStringBuilder.append("PRIMARY KEY (uuid)");
        sqlStringBuilder.append(" )");
        logEngine.write("Database","createTableTickets","-",sqlStringBuilder.toString());
        update(sqlStringBuilder.toString());
    }

    public String buildInsertSQLStatement(Ticket ticket) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("INSERT INTO tickets (uuid,date,time,source,destination,flight,ticket_class) VALUES (");
        sqlStringBuilder.append("'").append(ticket.getUUID()).append("'").append(",");
        sqlStringBuilder.append("'").append(ticket.getDate()).append("'").append(",");
        sqlStringBuilder.append("'").append(ticket.getTime()).append("'").append(",");
        sqlStringBuilder.append("'").append(ticket.getSource()).append("'").append(",");
        sqlStringBuilder.append("'").append(ticket.getDestination()).append("'").append(",");
        sqlStringBuilder.append("'").append(ticket.getFlight()).append("'").append(",");
        sqlStringBuilder.append("'").append(ticket.getTicketClass()).append("'");
        sqlStringBuilder.append(")");
        return sqlStringBuilder.toString();
    }

    public void insert(Ticket ticket) {
        logEngine.write("Database","insert","ticket = " + ticket.getUUID(),buildInsertSQLStatement(ticket));
        update(buildInsertSQLStatement(ticket));
    }

    public String buildUpdateSQLStatement(Ticket ticket) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("UPDATE tickets SET date = '").append(ticket.getDate()).append("'").append(",");
        sqlStringBuilder.append("time = '").append(ticket.getTime()).append("'").append(",");
        sqlStringBuilder.append("source = '").append(ticket.getSource()).append("'").append(",");
        sqlStringBuilder.append("destination = '").append(ticket.getDestination()).append("'").append(",");
        sqlStringBuilder.append("flight = '").append(ticket.getFlight()).append("'").append(",");
        sqlStringBuilder.append("ticket_class = '").append(ticket.getTicketClass()).append("'").append(",");
        sqlStringBuilder.append("gate = '").append(ticket.getGate()).append("'").append(",");
        sqlStringBuilder.append("seat = '").append(ticket.getSeat().getID()).append("' ");
        sqlStringBuilder.append("WHERE uuid = '").append(ticket.getUUID()).append("'");
        return sqlStringBuilder.toString();
    }

    public void innerUpdate(String dataPath,Ticket ticket) {
        startup(dataPath);
        logEngine.write("Database","innerUpdate","ticket = " + ticket.getUUID(),buildUpdateSQLStatement(ticket));
        update(buildUpdateSQLStatement(ticket));
        shutdown();
    }

    public String buildSelectTicketsSQLStatement() {
        return "SELECT uuid,date,time,source,destination,flight,ticket_class FROM tickets";
    }

    public ArrayList<Ticket> innerSelectTickets(String dataPath) {
        ArrayList<Ticket> tickets = new ArrayList<>();

        try {
            startup(dataPath);

            logEngine.write("Database","innerSelectTickets","-",buildSelectTicketsSQLStatement());
            ResultSet resultSet = select(buildSelectTicketsSQLStatement());
            while (resultSet.next()) {
                Ticket ticket = new Ticket(resultSet.getString("uuid"),resultSet.getString("date"),resultSet.getString("time"),
                        Source.valueOf(resultSet.getString("source")),Destination.valueOf(resultSet.getString("destination")),
                        resultSet.getString("flight"),TicketClass.valueOf(resultSet.getString("ticket_class")));
                tickets.add(ticket);
            }

            shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return tickets;
    }

    public String buildSelectTicketSQLStatement(Passenger passenger) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("SELECT uuid,date,time,source,destination,flight,ticket_class,gate FROM tickets ");
        sqlStringBuilder.append("INNER JOIN passengers ON tickets.uuid = passengers.ticket_uuid ");
        sqlStringBuilder.append("WHERE passengers.uuid = '").append(passenger.getUUID()).append("'");
        return sqlStringBuilder.toString();
    }

    public Ticket innerSelectTicket(String dataPath,Passenger passenger) {
        Ticket ticket = null;

        try {
            startup(dataPath);

            logEngine.write("Database","innerSelectTicket","passenger = " + passenger.getUUID(),buildSelectTicketSQLStatement(passenger));
            ResultSet resultSet = select(buildSelectTicketSQLStatement(passenger));
            while (resultSet.next())
                ticket = new Ticket(resultSet.getString("uuid"),resultSet.getString("date"),resultSet.getString("time"),
                        Source.valueOf(resultSet.getString("source")),Destination.valueOf(resultSet.getString("destination")),
                        resultSet.getString("flight"),TicketClass.valueOf(resultSet.getString("ticket_class")));

            shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return ticket;
    }

    // --- passengers

    public void dropTablePassengers() {
        String sqlStatement = "DROP TABLE passengers IF EXISTS";
        logEngine.write("Database","dropTablePassengers","-",sqlStatement);
        update(sqlStatement);
    }

    public void createTablePassengers() {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("CREATE TABLE passengers").append(" (");
        sqlStringBuilder.append("uuid VARCHAR(36) NOT NULL").append(",");
        sqlStringBuilder.append("name VARCHAR(15) NOT NULL").append(",");
        sqlStringBuilder.append("date_of_birth VARCHAR(10) NOT NULL").append(",");
        sqlStringBuilder.append("passport_id VARCHAR(10) NOT NULL").append(",");
        sqlStringBuilder.append("ticket_uuid VARCHAR(36)").append(",");
        sqlStringBuilder.append("PRIMARY KEY (uuid)");
        sqlStringBuilder.append(" )");
        logEngine.write("Database","createTablePassengers","-",sqlStringBuilder.toString());
        update(sqlStringBuilder.toString());
    }

    public void alterTablePassengersAddForeignKeyTicketUUID() {
        String sqlStatement = "ALTER TABLE passengers ADD FOREIGN KEY (ticket_uuid) REFERENCES tickets(uuid)";
        logEngine.write("Database","alterTablePassengersAddForeignKeyTicketUUID","-",sqlStatement);
        update(sqlStatement);
    }

    public String buildInsertSQLStatement(Passenger passenger) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("INSERT INTO passengers (uuid,name,date_of_birth,passport_id) VALUES (");
        sqlStringBuilder.append("'").append(passenger.getUUID()).append("'").append(",");
        sqlStringBuilder.append("'").append(passenger.getName()).append("'").append(",");
        sqlStringBuilder.append("'").append(passenger.getDateOfBirth()).append("'").append(",");
        sqlStringBuilder.append("'").append(passenger.getPassportID()).append("'");
        sqlStringBuilder.append(")");
        return sqlStringBuilder.toString();
    }

    public void insert(Passenger passenger) {
        logEngine.write("Database","insert","passenger = " + passenger.getUUID(),buildInsertSQLStatement(passenger));
        update(buildInsertSQLStatement(passenger));
    }

    public String buildUpdateSQLStatement(Passenger passenger) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("UPDATE passengers SET name = '").append(passenger.getName()).append("'").append(",");
        sqlStringBuilder.append("date_of_birth = '").append(passenger.getDateOfBirth()).append("'").append(",");
        sqlStringBuilder.append("passport_id = '").append(passenger.getPassportID()).append("'").append(",");
        sqlStringBuilder.append("ticket_uuid = '").append(passenger.getTicket().getUUID()).append("' ");
        sqlStringBuilder.append("WHERE uuid = '").append(passenger.getUUID()).append("'");
        return sqlStringBuilder.toString();
    }

    public void innerUpdate(String dataPath,Passenger passenger) {
        startup(dataPath);
        logEngine.write("Database","innerUpdate","passenger = " + passenger.getUUID(),buildUpdateSQLStatement(passenger));
        update(buildUpdateSQLStatement(passenger));
        shutdown();
    }

    public String buildSelectPassengersSQLStatement() {
        return "SELECT uuid,name,date_of_birth,passport_id FROM passengers";
    }

    public ArrayList<Passenger> innerSelectPassengers(String dataPath) {
        ArrayList<Passenger> passengers = new ArrayList<>();

        try {
            startup(dataPath);

            logEngine.write("Database","innerSelectPassengers","-",buildSelectPassengersSQLStatement());
            ResultSet resultSet = select(buildSelectPassengersSQLStatement());
            while (resultSet.next()) {
                Passenger passenger = new Passenger(resultSet.getString("uuid"),resultSet.getString("name"),resultSet.getString("date_of_birth"),resultSet.getString("passport_id"));
                passengers.add(passenger);
            }

            shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return passengers;
    }

    public String buildSelectPassengerSQLStatement(TicketClass ticketClass) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("SELECT uuid,name,date_of_birth,passport_id FROM passengers ");
        sqlStringBuilder.append("INNER JOIN tickets ON tickets.uuid = passengers.ticket_uuid ");
        sqlStringBuilder.append("WHERE tickets.ticket_class = '").append(ticketClass.toString()).append("'");
        return sqlStringBuilder.toString();
    }

    public ArrayList<Passenger> innerSelectPassengers(String dataPath,TicketClass ticketClass) {
        ArrayList<Passenger> passengers = new ArrayList<>();

        try {
            startup(dataPath);

            logEngine.write("Database","innerSelectPassengers","ticketClass = " + ticketClass,buildSelectPassengerSQLStatement(ticketClass));
            ResultSet resultSet = select(buildSelectPassengerSQLStatement(ticketClass));
            while (resultSet.next()) {
                Passenger passenger = new Passenger(resultSet.getString("uuid"),resultSet.getString("name"),resultSet.getString("date_of_birth"),resultSet.getString("passport_id"));
                passengers.add(passenger);
            }

            shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return passengers;
    }

    public String buildSelectPassengerSQLStatement(String uuid) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("SELECT uuid,name,date_of_birth,passport_id FROM passengers ");
        sqlStringBuilder.append("WHERE uuid = '").append(uuid).append("'");
        return sqlStringBuilder.toString();
    }

    public Passenger innerSelectPassenger(String dataPath,String uuid) {
        Passenger passenger = null;

        try {
            startup(dataPath);

            logEngine.write("Database","innerSelectPassenger","uuid = " + uuid,buildSelectPassengerSQLStatement(uuid));
            ResultSet resultSet = select(buildSelectPassengerSQLStatement(uuid));
            while (resultSet.next())
                passenger = new Passenger(resultSet.getString("uuid"),resultSet.getString("name"),resultSet.getString("date_of_birth"),resultSet.getString("passport_id"));

            shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return passenger;
    }

    // --- baggage

    public void dropTableBaggage() {
        String sqlStatement = "DROP TABLE baggage IF EXISTS";
        logEngine.write("Database","dropTableBaggage","-",sqlStatement);
        update(sqlStatement);
    }

    public void createTableBaggage() {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("CREATE TABLE baggage").append(" ( ");
        sqlStringBuilder.append("uuid VARCHAR(36) NOT NULL").append(",");
        sqlStringBuilder.append("content VARCHAR(50000) NOT NULL").append(",");
        sqlStringBuilder.append("weight INT NOT NULL").append(",");
        sqlStringBuilder.append("passenger_uuid VARCHAR(36)").append(",");
        sqlStringBuilder.append("PRIMARY KEY (uuid)");
        sqlStringBuilder.append(" )");
        logEngine.write("Database","createTableBaggage","-",sqlStringBuilder.toString());
        update(sqlStringBuilder.toString());
    }

    public void alterTablePassengersAddForeignKeyPassengerUUID() {
        String sqlStatement = "ALTER TABLE baggage ADD FOREIGN KEY (passenger_uuid) REFERENCES passengers(uuid)";
        logEngine.write("Database","alterTablePassengersAddForeignKeyPassengerUUID","-",sqlStatement);
        update(sqlStatement);
    }

    public String buildInsertSQLStatement(Baggage baggage) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("INSERT INTO baggage (uuid,content,weight) VALUES (");
        sqlStringBuilder.append("'").append(baggage.getUUID()).append("'").append(",");
        sqlStringBuilder.append("'").append(baggage.getContent()).append("'").append(",");
        sqlStringBuilder.append(baggage.getWeight());
        sqlStringBuilder.append(")");
        return sqlStringBuilder.toString();
    }

    public void insert(Baggage baggage) {
        logEngine.write("Database","insert","baggage = " + baggage.getUUID(),buildInsertSQLStatement(baggage));
        update(buildInsertSQLStatement(baggage));
    }

    public String buildUpdateSQLStatement(Baggage baggage) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("UPDATE baggage SET content = '").append(baggage.getContent()).append("'").append(",");
        sqlStringBuilder.append("weight = ").append(baggage.getWeight()).append(",");
        sqlStringBuilder.append("passenger_uuid = '").append(baggage.getPassenger().getUUID()).append("' ");
        sqlStringBuilder.append("WHERE uuid = '").append(baggage.getUUID()).append("'");
        return sqlStringBuilder.toString();
    }

    public void innerUpdate(String dataPath,Baggage baggage) {
        startup(dataPath);
        logEngine.write("Database","innerUpdate","baggage = " + baggage.getUUID(),buildUpdateSQLStatement(baggage));
        update(buildUpdateSQLStatement(baggage));
        shutdown();
    }

    public String buildSelectBaggageListSQLStatement() {
        return "SELECT uuid,content,weight FROM baggage";
    }

    public ArrayList<Baggage> innerSelectBaggageList(String dataPath) {
        ArrayList<Baggage> baggageList = new ArrayList<>();

        try {
            startup(dataPath);

            logEngine.write("Database","innerSelectBaggageList","-",buildSelectBaggageListSQLStatement());
            ResultSet resultSet = select(buildSelectBaggageListSQLStatement());
            while (resultSet.next()) {
                Baggage baggage = new Baggage(resultSet.getString(1),resultSet.getString(2),Double.parseDouble(resultSet.getString(3)));
                baggageList.add(baggage);
            }

            shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return baggageList;
    }

    public String buildSelectBaggageListSQLStatement(Passenger passenger) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("SELECT uuid,content,weight FROM baggage ");
        sqlStringBuilder.append("INNER JOIN passengers ON baggage.passenger_uuid = passengers.uuid ");
        sqlStringBuilder.append("WHERE passengers.uuid = '").append(passenger.getUUID()).append("'");
        return sqlStringBuilder.toString();
    }

    public ArrayList<Baggage> innerSelectBaggageList(String dataPath,Passenger passenger) {
        ArrayList<Baggage> baggageList = new ArrayList<>();

        try {
            startup(dataPath);

            logEngine.write("Database","innerSelectBaggageList","passenger = " + passenger.getUUID(),buildSelectBaggageListSQLStatement(passenger));
            ResultSet resultSet = select(buildSelectBaggageListSQLStatement(passenger));
            while (resultSet.next()) {
                Baggage baggage = new Baggage(resultSet.getString(1),resultSet.getString(2),Double.parseDouble(resultSet.getString(3)));
                baggageList.add(baggage);
            }

            shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return baggageList;
    }

    // --- baggage tag

    public void dropTableBaggageTags() {
        String sqlStatement = "DROP TABLE baggage_tags IF EXISTS";
        logEngine.write("Database","dropTableBaggageTags","-",sqlStatement);
        update(sqlStatement);
    }

    public void createTableBaggageTags() {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("CREATE TABLE baggage_tags").append(" (");
        sqlStringBuilder.append("uuid VARCHAR(36) NOT NULL").append(",");
        sqlStringBuilder.append("baggage_uuid VARCHAR(36)").append(",");
        sqlStringBuilder.append("status VARCHAR(10) DEFAULT 'unchecked' NOT NULL").append(",");
        sqlStringBuilder.append("PRIMARY KEY (uuid)");
        sqlStringBuilder.append(" )");
        logEngine.write("Database","createTableBaggageTags","-",sqlStringBuilder.toString());
        update(sqlStringBuilder.toString());
    }

    public void alterTableBaggageTagsAddForeignKeyBaggageUUID() {
        String sqlStatement = "ALTER TABLE baggage_tags ADD FOREIGN KEY (baggage_uuid) REFERENCES baggage(uuid)";
        logEngine.write("Database","alterTableBaggageTagsAddForeignKeyBaggageUUID","-",sqlStatement);
        update(sqlStatement);
    }

    public String buildInsertSQLStatement(BaggageTag baggageTag) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("INSERT INTO baggage_tags (uuid,baggage_uuid,status) VALUES (");
        sqlStringBuilder.append("'").append(baggageTag.getUUID()).append("'").append(",");
        sqlStringBuilder.append("'").append(baggageTag.getBaggage().getUUID()).append("'").append(",");
        sqlStringBuilder.append("'").append(baggageTag.getBaggageStatus()).append("'");
        sqlStringBuilder.append(")");
        return sqlStringBuilder.toString();
    }

    public void innerInsert(String dataPath,BaggageTag baggageTag) {
        startup(dataPath);
        logEngine.write("Database","innerInsert","baggageTag = " + baggageTag.getUUID(),buildInsertSQLStatement(baggageTag));
        update(buildInsertSQLStatement(baggageTag));
        shutdown();
    }

    public String buildUpdateSQLStatement(BaggageTag baggageTag) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("UPDATE baggage_tags SET baggage_uuid = '").append(baggageTag.getBaggage().getUUID()).append("'").append(",");
        sqlStringBuilder.append("status = '").append(baggageTag.getBaggageStatus()).append("' ");
        sqlStringBuilder.append("WHERE uuid = '").append(baggageTag.getUUID()).append("'");
        return sqlStringBuilder.toString();
    }

    public void innerUpdate(String dataPath,BaggageTag baggageTag) {
        startup(dataPath);
        logEngine.write("Database","innerUpdate","baggageTag = " + baggageTag.getUUID(),buildUpdateSQLStatement(baggageTag));
        update(buildUpdateSQLStatement(baggageTag));
        shutdown();
    }

    public String buildSelectBaggageTagSQLStatement(Baggage baggage) {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("SELECT uuid,baggage_uuid,status FROM baggage_tags ");
        sqlStringBuilder.append("INNER JOIN baggage ON baggage_tags.baggage_uuid = baggage.uuid ");
        sqlStringBuilder.append("WHERE baggage.uuid = '").append(baggage.getUUID()).append("'");
        return sqlStringBuilder.toString();
    }

    public BaggageTag innerSelectBaggageTag(String dataPath,Baggage baggage) {
        BaggageTag baggageTag = null;

        try {
            startup(dataPath);

            logEngine.write("Database","innerSelectBaggageTag","baggage = " + baggage.getUUID(),buildSelectBaggageTagSQLStatement(baggage));
            ResultSet resultSet = select(buildSelectBaggageTagSQLStatement(baggage));
            while (resultSet.next())
                baggageTag = new BaggageTag(resultSet.getString("uuid"),baggage,BaggageStatus.valueOf(resultSet.getString("status")));

            shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return baggageTag;
    }

    public void dropTables(boolean isTableSeatsDropped,
                           boolean isTablePassengersDropped,
                           boolean isTableTicketsDropped,
                           boolean isTableBaggageDropped,
                           boolean isTableBaggageTagsDropped) {
        if (isTableBaggageTagsDropped)
            dropTableBaggageTags();

        if (isTableBaggageDropped)
            dropTableBaggage();

        if (isTablePassengersDropped)
            dropTablePassengers();

        if (isTableTicketsDropped)
            dropTableTickets();

        if (isTableSeatsDropped)
            dropTableSeats();

        System.out.println();
    }

    public void createTables(boolean isTableSeatsCreated,
                             boolean isTablePassengersCreated,
                             boolean isTableTicketsCreated,
                             boolean isTableBaggageCreated,
                             boolean isTableBaggageTagsCreated) {
        if (isTableSeatsCreated)
            createTableSeats();

        if (isTableTicketsCreated)
            createTableTickets();

        if (isTablePassengersCreated) {
            createTablePassengers();
            alterTablePassengersAddForeignKeyTicketUUID();
        }

        if (isTableBaggageCreated) {
            createTableBaggage();
            alterTablePassengersAddForeignKeyPassengerUUID();
        }

        if (isTableBaggageTagsCreated) {
            createTableBaggageTags();
            alterTableBaggageTagsAddForeignKeyBaggageUUID();
        }

        System.out.println();
    }

    public void innerSetup(String dataPath,boolean seats,boolean passengers,boolean tickets,
                           boolean baggage,boolean baggage_tag) {
        startup(dataPath);
        dropTables(seats,passengers,tickets,baggage,baggage_tag);
        createTables(seats,passengers,tickets,baggage,baggage_tag);
        shutdown();
    }

    // import

    public void innerImportCSVDataSeats(String dataPath) {
        try {
            startup(dataPath);

            BufferedReader bufferedReader = new BufferedReader(new FileReader(Configuration.instance.dataPath + "seat_configuration_A380.csv"));
            String line;
            String[] strings = null;
            while ((line = bufferedReader.readLine()) != null)
                strings = line.split(";");

            TicketClass ticketClass = null;

            for (int i = 0;i < strings.length;i++) {
                String string = strings[i];
                String prefix = string.substring(0,1);

                switch (prefix) {
                    case "F": ticketClass = TicketClass.FIRST;
                    break;
                    case "B": ticketClass = TicketClass.BUSINESS;
                    break;
                    case "E": ticketClass = TicketClass.ECONOMY;
                }

                Seat seat = new Seat(UUID.randomUUID().toString(),ticketClass,string.substring(1,string.length()),"y");
                insert(seat);
            }

            shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void innerImportCSVDataTickets(String dataPath) {
        try {
            startup(dataPath);

            BufferedReader bufferedReader = new BufferedReader(new FileReader(Configuration.instance.dataPath + "tickets.csv"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] strings = line.split(";");
                Ticket ticket = new Ticket(strings[0],strings[1],strings[2],Source.valueOf(strings[3]),Destination.valueOf(strings[4]),strings[5],
                        TicketClass.valueOf(strings[6]));
                insert(ticket);
            }

            shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void innerImportCSVDataPassengers(String dataPath) {
        try {
            startup(dataPath);

            BufferedReader bufferedReader = new BufferedReader(new FileReader(Configuration.instance.dataPath + "passengers.csv"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] strings = line.split(";");
                Passenger passenger = new Passenger(strings[0],strings[1],strings[2],strings[3]);
                insert(passenger);
            }

            shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void innerImportCSVDataBaggage(String dataPath) {
        try {
            startup(dataPath);

            BufferedReader bufferedReader = new BufferedReader(new FileReader(Configuration.instance.dataPath + "baggage.csv"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] strings = line.split(";");
                Baggage baggage = new Baggage(strings[0],strings[1],Double.parseDouble(strings[2]));
                insert(baggage);
            }

            shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        try {
            Statement statement = connection.createStatement();
            statement.execute("SHUTDOWN");
            connection.close();
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        }
    }
}