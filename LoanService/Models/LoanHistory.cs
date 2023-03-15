using System;

public class LoanHistory {
    public class Entry {
        public DateTime LoanTimestamp { get; set; }
        public bool Active { get; set; }
        public int UserId { get; set; }
        public string Username { get; set; }
    }
    public string ISBN { get; set; }
    public string Title { get; set; }
    public string Author { get; set; }
    public DateTime PublicationDate { get; set; }
    public string Price { get; set; }
    public Entry[] History { get; set; }
}