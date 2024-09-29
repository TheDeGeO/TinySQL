namespace ApiInterface.InternalModels
{
    internal enum RequestType 
    { 
        SQLSentence = 0
    }

    internal class Request
    {
        public required RequestType RequestType { get; set; } 

        public required string RequestBody { get; set; }
    }
}
