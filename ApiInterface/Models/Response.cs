using ApiInterface.InternalModels;
using Entities;

namespace ApiInterface.Models
{
    internal class Response
    {
        public required Request Request { get; set; }

        public required OperationStatus Status { get; set; }

        public required string ResponseBody { get; set; }
    }
}
