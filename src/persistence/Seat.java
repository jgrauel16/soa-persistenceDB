public class Seat {
    private String uuid;
    private TicketClass ticketClass;
    private String id;
    private String available;

    public Seat(String uuid,TicketClass ticketClass,String id,String available) {
        this.uuid = uuid;
        this.ticketClass = ticketClass;
        this.id = id;
        this.available = available;
    }

    public String getUUID() {
        return uuid;
    }

    public TicketClass getTicketClass() {
        return ticketClass;
    }

    public String getID() {
        return id;
    }

    public String getAvailable() {
        return available;
    }

    public void setAvailable(String available) {
        this.available = available;
    }
}