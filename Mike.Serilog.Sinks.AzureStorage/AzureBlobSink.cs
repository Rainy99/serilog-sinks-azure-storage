using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Sinks.PeriodicBatching;

namespace Mike.Serilog.Sinks.AzureStorage
{
    public class AzureBlobSink : PeriodicBatchingSink
    {
        private readonly ITextFormatter _formatter;

        private readonly CloudAppendBlob _blob;

        public AzureBlobSink(ITextFormatter formatter,
                             CloudStorageAccount storageAccount,
                             string containerName,
                             string blobName,
                             int batchSizeLimit,
                             TimeSpan period) : base(batchSizeLimit, period)
        {
            _formatter = formatter;

            // Create the blob client
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // Retrieve blob reference
            _blob = container.GetAppendBlobReference(blobName);
        }

        protected override async Task EmitBatchAsync(IEnumerable<LogEvent> events)
        {
            Stream stream;

            try
            {
                stream = await _blob.OpenWriteAsync(false);
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 404)
            {
                // When the blob doesn't already exist, create it
                stream = await _blob.OpenWriteAsync(true);
            }

            using (StreamWriter writer = new StreamWriter(stream))
            {
                foreach (LogEvent evnt in events)
                {
                    _formatter.Format(evnt, writer);
                }
            }
        }
    }
}