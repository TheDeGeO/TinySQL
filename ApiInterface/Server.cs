using System.Net.Sockets;
using System.Net;
using System.Text;
using ApiInterface.InternalModels;
using System.Text.Json;
using ApiInterface.Exceptions;
using ApiInterface.Processors;
using ApiInterface.Models;

namespace ApiInterface
{
    public class Server
    {
        private static IPEndPoint serverEndPoint = new(IPAddress.Loopback, 11000);
        private static int supportedParallelConnections = 1;

        public static async Task Start()
        {
            using Socket listener = new(serverEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            listener.Bind(serverEndPoint);
            listener.Listen(supportedParallelConnections);
            Console.WriteLine($"Server ready at {serverEndPoint.ToString()}");

            while (true)
            {
                var handler = await listener.AcceptAsync();
                try
                {
                    var rawMessage = GetMessage(handler);
                    var requestObject = ConvertToRequestObject(rawMessage);
                    var response = ProcessRequest(requestObject);
                    SendResponse(response, handler);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    await SendErrorResponse("Unknown exception", handler);
                }
                finally
                {
                    handler.Close();
                }
            }
        }

        private static string GetMessage(Socket handler)
        {
            using (NetworkStream stream = new NetworkStream(handler))
            using (StreamReader reader = new StreamReader(stream))
            {
                return reader.ReadLine() ?? String.Empty;
            }
        }

        private static Request ConvertToRequestObject(string rawMessage)
        {
            return JsonSerializer.Deserialize<Request>(rawMessage) ?? throw new InvalidRequestException();
        }

        private static Response ProcessRequest(Request requestObject)
        {
            var processor = ProcessorFactory.Create(requestObject);
            return processor.Process();
        }

        private static void SendResponse(Response response, Socket handler)
        {
            using (NetworkStream stream = new NetworkStream(handler))
            using (StreamWriter writer = new StreamWriter(stream))
            {
                writer.WriteLine(JsonSerializer.Serialize(response));
            }
        }

        private static Task SendErrorResponse(string reason, Socket handler)
        {
            throw new NotImplementedException();
        }

        
    }
}
