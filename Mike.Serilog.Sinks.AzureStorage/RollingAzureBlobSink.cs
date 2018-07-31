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
    public class RollingAzureBlobSink : PeriodicBatchingSink
    {
        private readonly ITextFormatter _formatter;

        private readonly CloudBlobContainer _container;
        private readonly string _baseBlobName;
        private CloudAppendBlob _blob;
        private DateTime _day;

        public RollingAzureBlobSink(ITextFormatter formatter,
                                    CloudStorageAccount storageAccount,
                                    string containerName,
                                    string baseBlobName,
                                    int batchSizeLimit,
                                    TimeSpan period) : base(batchSizeLimit, period)
        {
            _formatter = formatter;

            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            _container = blobClient.GetContainerReference(containerName);
            _baseBlobName = baseBlobName;

            Roll();
        }

        protected override async Task EmitBatchAsync(IEnumerable<LogEvent> events)
        {
            // If the day has changed, roll along to a new blob
            if (_day.Date != DateTime.Now.Date)
            {
                Roll();
            }

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

        private void Roll()
        {
            _day = DateTime.Now;
            string stamp = _day.ToString("yyyy-MM-dd");
            string blobName = $"{_baseBlobName}_{stamp}.log";
            _blob = _container.GetAppendBlobReference(blobName);

            _blob.CreateOrReplaceAsync().Wait();

            _blob.FetchAttributesAsync().Wait();

            _blob.Metadata["Day"] = stamp;
            _blob.Metadata["Name"] = _baseBlobName;

            _blob.SetMetadataAsync().Wait();
        }
    }
}