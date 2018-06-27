using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;

namespace TableEventSimulator
{
    // The TableData class holds the object that will be serialized to the event hub to simulate the data from the table system
    public class TableData
    {
        // The sample provided did not list any info for supervisor data, so we will just populate it with the word, "Nobody"
        public TableData()
        {
            c_supervisorid = "Nobody";
            c_supervisorfirstname = "Nobody";
            c_supervisorlastname = "Nobody";
            i_minbet = 2;
        }

        public string c_prop_cd { get; set; }
        public int c_tableid { get; set; }
        public DateTime d_headcount_datetime { get; set; }
        public int i_shift { get; set; }
        public string c_openclose { get; set; }
        public int c_pitnum { get; set; }
        public int c_dealerid { get; set; }
        public string c_dealerfirstname { get; set; }
        public string c_dealerlastname { get; set; }
        public string c_supervisorid { get; set; }
        public string c_supervisorfirstname { get; set; }
        public string c_supervisorlastname { get; set; }
        public int i_gamedate { get; set; }
        public int i_openratings { get; set; }
        public int i_headcount_total { get; set; }
        public int i_minbet { get; set; }
        public int i_seats_available { get; set; }

    }
    class Program
    {
        // By default, we'll send 10 messages
        private static int numMessages = 10;
        //This is the anchor class for all Azure Event Hub operations. It is found in Microsoft.Azure.Eventhubs and documentation is here: https://docs.microsoft.com/en-us/dotnet/api/overview/azure/event-hubs?view=azure-dotnet 
        private static EventHubClient eventHubClient;
        //This is the event hub namespace connection string. 
        private const string EventHubConnectionString = "Endpoint=sb://caesarsdemo.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Xt24BdnKeZDjoPAe2Xxait/aVTOv4jUHhkv3Y9v0Nko=";
        //This is the name of the event hub that will receive data
        private const string EventHubName = "TableData";
        //Since we're going to run async, we simply call MainAsync from the Main method and pass all original arguments
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }
        //The primary entry point for the application
        private static async Task MainAsync(string[] args)
        {
            if (args.Length != 0)
            {
                numMessages = Convert.ToInt32(args[0]);
            }
            // Creates an EventHubsConnectionStringBuilder object from a the connection string, and sets the EntityPath.
            // Typically the connection string should have the Entity Path in it, but for the sake of this simple scenario
            // we are using the connection string from the namespace.
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            //Open up an event hub connection
            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            //Async call to send messages (passing the number of messages as a parameter)
            await SendMessagesToEventHub(numMessages);

            //Once we've finished sending messages, close the client down
            await eventHubClient.CloseAsync();

            Console.WriteLine("Press the enter key to exit.");
            Console.ReadLine();
        }

        //This is the primary worker method for the application. Basically it instantiates a TableData object, assigns random values to the properties, serializes the object to JSON, and then sends it to event hub.
        //It loops until either the requested number of messages has been sent, or an exception occurs. 
        private static async Task SendMessagesToEventHub(int numMessagesToSend)
        {
            for (var i = 0; i < numMessagesToSend; i++)
            {
                // Easiest way to generate a random number in C#
                Random rnd = new Random();
                //Instantiate a new instance of TableData
                TableData tdata = new TableData();
                //Randomly assign the number of seats occupied
                int seatsOccupied = rnd.Next(9);
                //Randomly choose a property code
                int propCode = rnd.Next(1, 10);
                //Randomly choose a dealer code
                int dealerCode = rnd.Next(1, 5);
                //Turn the randomly chosen property code into a character code
                switch (propCode)
                {
                    case 1: tdata.c_prop_cd = "CHE";
                        break;
                    case 2: tdata.c_prop_cd = "CAC";
                        break;
                    case 3: tdata.c_prop_cd = "BAC";
                        break;
                    case 4: tdata.c_prop_cd = "BLV";
                        break;
                    case 5: tdata.c_prop_cd = "FLM";
                        break;
                    case 6: tdata.c_prop_cd = "PAR";
                        break;
                    case 7: tdata.c_prop_cd = "PHR";
                        break;
                    case 8: tdata.c_prop_cd = "CRO";
                        break;
                    case 9: tdata.c_prop_cd = "LIN";
                        break;
                    case 10: tdata.c_prop_cd = "HLV";
                        break;
                }

                //Based on the random number for the dealer, assign appropriate dealer information
                switch (dealerCode)
                {
                    case 1: tdata.c_dealerid = 321810;
                        tdata.c_dealerlastname = "Sacarias";
                        tdata.c_dealerfirstname = "Mark";
                        break;
                    case 2: tdata.c_dealerid = 329340;
                        tdata.c_dealerfirstname = "Lloyd";
                        tdata.c_dealerlastname = "Richards";
                        break;
                    case 3: tdata.c_dealerid = 246222;
                        tdata.c_dealerfirstname = "Austin";
                        tdata.c_dealerlastname = "Bissell";
                        break;
                    case 4: tdata.c_dealerid = 61671;
                        tdata.c_dealerfirstname = "Angela";
                        tdata.c_dealerlastname = "Wolfe";
                        break;
                    case 5: tdata.c_dealerid = 318666;
                        tdata.c_dealerfirstname = "Hare";
                        tdata.c_dealerlastname = "Sukhjinder";
                        break;
                }

                //Randomly pick a Pit number
                tdata.c_pitnum = rnd.Next(1, 9);
                //Choose a table ID (get close to the real table IDs)
                tdata.c_tableid = rnd.Next(90900, 90999);
                //Assign the UTC DateTime to the object
                tdata.d_headcount_datetime = System.DateTime.UtcNow;
                //Randomly choose a shift (this should probably have been based on datetime, but I was lazy)
                tdata.i_shift = rnd.Next(1, 4);
                //All of this data is on Open of the table as opposed to Close
                tdata.c_openclose = "O";
                //This was a value that was provided in the sample data. Not sure what it means actually
                tdata.i_gamedate = 43171;
                //Choose the number of seats occupied on open
                tdata.i_openratings = seatsOccupied;
                tdata.i_headcount_total = seatsOccupied;
                //If each table has 9 seats, figure out how many are available out of the randomly chosen headcount
                tdata.i_seats_available = 9 - seatsOccupied;


                try
                {   //Serialize the object to JSON                
                    var message = JsonConvert.SerializeObject(tdata);
                    //Display the message in the console
                    Console.WriteLine($"Sending message {i.ToString()}: {message}");
                    //Async send of the data to the event hub using the EventData method.
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch(Exception ex)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {ex.Message}");
                }
                //Delay 2 seconds between message delivery. This just keeps our demo from requiring a higher pricing tier
                await Task.Delay(2000);
                
            }
            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }
    }
}
