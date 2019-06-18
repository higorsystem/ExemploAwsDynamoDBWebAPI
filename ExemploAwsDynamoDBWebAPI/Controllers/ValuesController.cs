using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.AspNetCore.Mvc;

namespace ExemploAwsDynamoDBWebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        private const string TableName = "DynamoDBAlterdataAnalise";      

        private List<ContratoPessoa> listaContratoPessoas;

        private readonly IAmazonDynamoDB _amazonDynamoDb;

        public ValuesController(IAmazonDynamoDB amazonDynamoDb)
        {
            _amazonDynamoDb = amazonDynamoDb;
            listaContratoPessoas = new List<ContratoPessoa>();
        }

        // GET api/values/init
        [HttpGet]
        [Route("init")]
        public async Task Initialise()
        {
            var request = new ListTablesRequest
            {
                Limit = 10
            };

            var response = await _amazonDynamoDb.ListTablesAsync(request);

            var results = response.TableNames;

            if (!results.Contains(TableName))
            {
                var createRequest = new CreateTableRequest
                {
                    TableName = TableName,
                    AttributeDefinitions = new List<AttributeDefinition>
                    {
                        new AttributeDefinition
                        {
                            AttributeName = "Id",
                            AttributeType = "N"
                        }
                    },
                    KeySchema = new List<KeySchemaElement>
                    {
                        new KeySchemaElement
                        {
                            AttributeName = "Id",
                            KeyType = KeyType.HASH  //Partition key
                        }
                    },
                    ProvisionedThroughput = new ProvisionedThroughput
                    {
                        ReadCapacityUnits = 5,
                        WriteCapacityUnits = 5
                    },                    
                };

                 await _amazonDynamoDb.CreateTableAsync(createRequest);
            }
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public async Task<ActionResult<string>> Get(int id)
        {
            var request = new GetItemRequest
            {
                TableName = TableName,
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { N = id.ToString() } } }
            };

            var response = await _amazonDynamoDb.GetItemAsync(request);

            if (!response.IsItemSet)
                return NotFound();

            return response.Item["Titulo"].S;
        }

        [HttpGet]
        [Route("all")]
        public async Task<IActionResult> GetAll(int? id)
        {
            var queryRequest = RequestBuilder(id); 

            var response = await ScanAsync(queryRequest);  

            listaContratoPessoas.AddRange(response.Items.Select(Map));

            return Ok(listaContratoPessoas.OrderBy(x => x.Id).ToList());
        }

        private async Task<ScanResponse> ScanAsync(ScanRequest request)
        {
            var response = await _amazonDynamoDb.ScanAsync(request);

            return response;
        }

        private ContratoPessoa Map(Dictionary<string, AttributeValue> result)
        {
            var objPessoa =  new ContratoPessoa
            {
                Id = Convert.ToInt32(result["Id"].N),
                Titulo = result["Titulo"].S,
                Nome = result["Nome"].S
            };

            return objPessoa;
        }

         private ScanRequest RequestBuilder(int? id)
        {
            if (id.HasValue == false)
            {
                return new ScanRequest
                {
                    TableName = "DynamoDBAlterdataAnalise"
                };
            }

            return new ScanRequest
            {
                TableName = "DynamoDBAlterdataAnalise",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {
                        ":v_Id", new AttributeValue { N = id.ToString()}}

                },
                FilterExpression = "Id = :v_Id",
                ProjectionExpression = "Id, Titulo, Nome"
            };
        }

        // POST api/values
        [HttpPost]
        public async Task Post([FromBody] ContratoPessoa input)
        {            
            var request = new PutItemRequest
            {
                TableName = TableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { N = input.Id.ToString() }},
                    { "Titulo", new AttributeValue { S = input.Titulo }},
                    { "Nome", new AttributeValue { S = input.Nome }}
                }
            };

            await _amazonDynamoDb.PutItemAsync(request);
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            var request = new DeleteItemRequest
            {
                TableName = TableName,
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { N = id.ToString() } } }
            };

            var response = await _amazonDynamoDb.DeleteItemAsync(request);

            return StatusCode((int)response.HttpStatusCode);
        }

        [HttpDelete]
        [Route("delete")]
        public async Task<IActionResult> DeleteAll()
        {
            var queryRequest = RequestBuilder(null); 

            var response = await ScanAsync(queryRequest);  

            var deletado = await _amazonDynamoDb.DeleteTableAsync(TableName);

            return Ok(deletado);
        }
    }
}
