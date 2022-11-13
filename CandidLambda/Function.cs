using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SimpleNotificationService.Util;
using CandidLambda.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Text.Json.Serialization;
using System.Threading.Tasks;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace CandidLambda
{
    public class Function
    {
        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {

        }


        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
        /// to respond to SQS messages.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SQSEvent evnt, ILambdaContext context)
        {
            foreach (var message in evnt.Records)
            {
                await ProcessMessageAsync(message, context);
            }
        }

        private async Task ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context)
        {         
            //Use Stored procedure to pull id from database
            var doc = await PullIdFromSql(new SqlConnection(@"Server =(localdb)\MSSQLLocalDB;Database=Candid;Integrated Security=True"), "CandidGetProc", message.Body);
            if (doc == null)
            {
                await Task.CompletedTask;
                context.Logger.LogLine($"Message was not processed because it was not found in SQL {message.Body}");
                return;
            }
            var timeStampedDoc = new TimeStampedDoc();
            timeStampedDoc = JsonConvert.DeserializeObject<TimeStampedDoc>(doc);

            //Store in DynamoDB              
            var dynamoDB = new AmazonDynamoDBClient();
            var result = await PutTimeStampedItemAsync(dynamoDB, timeStampedDoc, "test-table");

            //Call SNS topic 
            var sns = new AmazonSimpleNotificationServiceClient();             
            var result2 = await SendProcessedNotificationAsync(sns, timeStampedDoc.id);


            context.Logger.LogLine($"Processed message {message.Body}");
            await Task.CompletedTask;            
        }

        public static async Task<string> PullIdFromSql(SqlConnection conn, string StoredProcedureName, string docID)        
        {
            //connect to sql            
            using (conn)
            {
                conn.Open();

                var cmd = new SqlCommand("CandidGetProc", conn);
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.Parameters.Add(new SqlParameter("docID", docID));

                //execute stored proc
                var doc = await cmd.ExecuteScalarAsync();

                if (doc == null)
                {
                    return null;
                }
                else
                    return doc.ToString();

                
            }

        }
                
        public static async Task<bool> PutTimeStampedItemAsync(IAmazonDynamoDB client, TimeStampedDoc doc, string tableName)
        {
            var item = new Dictionary<string, AttributeValue>
            {
                ["id"] = new AttributeValue { S = doc.id },
                ["timestamp"] = new AttributeValue { S = doc.timestamp.ToString("G") },
                ["name"] = new AttributeValue { S = doc.name }
            };

            var request = new PutItemRequest
            {
                TableName = tableName,
                Item = item,
            };

            var response = await client.PutItemAsync(request);
            return response.HttpStatusCode == System.Net.HttpStatusCode.OK;
        }

        public static async Task<bool> SendProcessedNotificationAsync(IAmazonSimpleNotificationService snsClient, string id)
        {
            var request = new PublishRequest
            {                
                TopicArn = "arn:aws:sns:us-east-1:612419466211:CandidTopic",
                Message = $"Processed id: {id}"
            };

            var response = await snsClient.PublishAsync(request);
            return response.HttpStatusCode == System.Net.HttpStatusCode.OK;
        }



    }
}