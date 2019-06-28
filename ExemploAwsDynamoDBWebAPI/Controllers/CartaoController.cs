using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using ExemploAwsDynamoDBWebAPI.DTO;
using ExemploAwsDynamoDBWebAPI.Parser;
using Microsoft.AspNetCore.Mvc;

namespace ExemploAwsDynamoDBWebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CartaoController : ControllerBase
    {
        private const string TableName = "Cartao";      

        private List<CartaoDTO> listaDtoCartao;

        private readonly IAmazonDynamoDB _amazonDynamoDb;

        public CartaoController(IAmazonDynamoDB amazonDynamoDb)
        {
            _amazonDynamoDb = amazonDynamoDb;
            listaDtoCartao = new List<CartaoDTO>();
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
                            AttributeType = "S"
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
        public async Task<ActionResult> Get(int id)
        {
            var request = new GetItemRequest
            {
                TableName = TableName,
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { N = id.ToString() } } }
            };

            var response = await _amazonDynamoDb.GetItemAsync(request);

            if (!response.IsItemSet)
                return NotFound();

            return Ok(CartaoParser.Parser(response.Item));
        }

        [HttpGet]
        [Route("all")]
        public async Task<IActionResult> GetAll()
        {
            var queryRequest = RequestBuilder(null); 

            var response = await ScanAsync(queryRequest);  

            var teste = response.Items;           

            listaDtoCartao.AddRange(response.Items.Select(Map));

            return Ok(listaDtoCartao.OrderBy(x => x.Id).ToList());
        }

        private async Task<ScanResponse> ScanAsync(ScanRequest request)
        {
            var response = await _amazonDynamoDb.ScanAsync(request);

            return response;
        }

        private CartaoDTO Map(Dictionary<string, AttributeValue> result)
        {
            var objPessoa =  new CartaoDTO
            {
                Id = Convert.ToInt32(result["Id"].N),
                Token = result["TokenCartao"].S
            };               

            return objPessoa;
        }

         private ScanRequest RequestBuilder(int? id)
        {
            if (id.HasValue == false)
            {
                return new ScanRequest
                {
                    TableName = "Cartao"
                };
            }

            return new ScanRequest
            {
                TableName = "Cartao",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {
                        ":v_Id", new AttributeValue { N = id.ToString()}}

                },
                FilterExpression = "Id = :v_Id",
                ProjectionExpression = "Id, TokenCartao"
            };
        }

        // POST api/values
        [HttpPost]
        public async Task Post([FromBody] CartaoDTO input)
        {   
            this.GerarIdAutomatico(input);
            var request = new PutItemRequest
            {
                TableName = TableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { N = input.Id.ToString() }},
                    { "TokenCartao", new AttributeValue { S = input.Token }}                    
                }
            };

            await _amazonDynamoDb.PutItemAsync(request);
        }

        private void GerarIdAutomatico(CartaoDTO contrato)
        {
            Random rd = new Random();
            contrato.Id = rd.Next(12, 1000000);                
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
