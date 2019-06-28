using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using ExemploAwsDynamoDBWebAPI.DTO;

namespace ExemploAwsDynamoDBWebAPI.Parser
{
    public class CartaoParser
    {
        public static CartaoDTO Parser(Dictionary<string, AttributeValue> resultado)
        {
            if(resultado == null)
            {
                return null;
            }

            var contrato = new CartaoDTO();
            contrato.Id = Convert.ToInt32(resultado["Id"].N);
            contrato.Token = resultado["Token"].S;       
            
            return contrato;
        }

        public List<CartaoDTO> Parser(List<Dictionary<string, AttributeValue>> entidadesPessoas)
        {           
            return entidadesPessoas.Select(Parser).ToList();
        }
    }
}