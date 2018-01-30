import java.util.ArrayList;

public interface IDatabase {
    void setLogEngine(LogEngine logEngine);
    void setup(String dataPath,boolean seats,boolean passengers,boolean tickets,boolean baggage,boolean baggageTag);
    void resetSeats(String dataPath);
    void resetTickets(String dataPath);
    void importCSVDataSeats(String dataPath);
    Seat selectSeat(String dataPath,TicketClass ticketClass);
    void update(String dataPath,Seat seat);
    void importCSVDataTickets(String dataPath);
    void update(String dataPath,Ticket ticket);
    ArrayList<Ticket> selectTickets(String dataPath);
    Ticket selectTicket(String dataPath,Passenger passenger);
    void importCSVDataPassengers(String dataPath);
    void update(String dataPath,Passenger passenger);
    ArrayList<Passenger> selectPassengers(String dataPath);
    ArrayList<Passenger> selectPassengers(String dataPath,TicketClass ticketClass);
    Passenger selectPassenger(String dataPath,String uuid);
    void importCSVDataBaggage(String dataPath);
    ArrayList<Baggage> selectBaggageList(String dataPath);
    ArrayList<Baggage> selectBaggageList(String dataPath,Passenger passenger);
    void update(String dataPath,Baggage baggage);
    void insert(String dataPath,BaggageTag baggageTag);
    void update(String dataPath,BaggageTag baggageTag);
    BaggageTag selectBaggageTag(String dataPath,Baggage baggage);
}