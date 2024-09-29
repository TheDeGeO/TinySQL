using ApiInterface.InternalModels;
using ApiInterface.Models;
using Entities;
using QueryProcessor;

namespace ApiInterface.Processors
{
    internal class SQLSentenceProcessor(Request request) : IProcessor 
    {
        public Request Request { get; } = request;

        public Response Process()
        {
            var sentence = this.Request.RequestBody;
            var result = SQLQueryProcessor.Execute(sentence);
            var response = this.ConvertToResponse(result);
            return response;
        }

        private Response ConvertToResponse(OperationStatus result)
        {
            return new Response
            {
                Status = result,
                Request = this.Request,
                ResponseBody = string.Empty
            };
        }
    }
}
