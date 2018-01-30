import java.util.ArrayList;

public class Passenger {
    private String uuid;
    private String name;
    private String dateOfBirth;
    private String passportID;
    private Ticket ticket;
    private ArrayList<Baggage> baggageList;

    public Passenger(String uuid,String name,String dateOfBirth,String passportID) {
        this.uuid = uuid;
        this.name = name;
        this.dateOfBirth = dateOfBirth;
        this.passportID = passportID;
        baggageList = new ArrayList<>();
    }

    public String getUUID() {
        return uuid;
    }

    public String getName() {
        return name;
    }

    public String getDateOfBirth() {
        return dateOfBirth;
    }

    public String getPassportID() {
        return passportID;
    }

    public void setTicket(Ticket ticket) {
        this.ticket = ticket;
    }

    public Ticket getTicket() {
        return ticket;
    }

    public ArrayList<Baggage> getBaggageList() {
        return baggageList;
    }
}