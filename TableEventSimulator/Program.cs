using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;

namespace TableEventSimulator
{
    public class TableData
    {
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
        private static int numMessages = 10;
        private static EventHubClient eventHubClient;
        private const string EventHubConnectionString = "Endpoint=sb://caesarsdemo.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Xt24BdnKeZDjoPAe2Xxait/aVTOv4jUHhkv3Y9v0Nko=";
        private const string EventHubName = "TableData";
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }
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

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            await SendMessagesToEventHub(numMessages);

            await eventHubClient.CloseAsync();

            Console.WriteLine("Press the enter key to exit.");
            Console.ReadLine();
        }

        private static async Task SendMessagesToEventHub(int numMessagesToSend)
        {
            for (var i = 0; i < numMessagesToSend; i++)
            {
                Random rnd = new Random();
                TableData tdata = new TableData();
                int seatsOccupied = rnd.Next(9);
                int propCode = rnd.Next(1, 10);
                int dealerCode = rnd.Next(1, 5);
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

                tdata.c_pitnum = rnd.Next(1, 9);
                tdata.c_tableid = rnd.Next(90900, 90999);
                tdata.d_headcount_datetime = System.DateTime.UtcNow;
                tdata.i_shift = rnd.Next(1, 4);
                tdata.c_openclose = "O";
                tdata.i_gamedate = 43171;
                tdata.i_openratings = seatsOccupied;
                tdata.i_headcount_total = seatsOccupied;
                tdata.i_seats_available = 9 - seatsOccupied;


                try
                {                   
                    var message = JsonConvert.SerializeObject(tdata);
                    Console.WriteLine($"Sending message: {message}");
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch(Exception ex)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {ex.Message}");
                }
                await Task.Delay(2000);
                
            }
            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }
    }
}
