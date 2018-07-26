using System;
using Microsoft.WindowsAzure.Storage;
using Serilog;
using Serilog.Configuration;
using Serilog.Formatting;

namespace Mike.Serilog.Sinks.AzureStorage
{
    public static class AzureBlobSinkExtensions
    {
        public static LoggerConfiguration AzureBlobSink(this LoggerSinkConfiguration loggerConfiguration,
                                                        ITextFormatter formatter,
                                                        CloudStorageAccount storageAccount,
                                                        string containerName,
                                                        string blobName,
                                                        int batchSizeLimit,
                                                        TimeSpan period)
        {
            return loggerConfiguration.Sink(
                new AzureBlobSink(formatter, storageAccount, containerName, blobName, batchSizeLimit, period));
        }

        public static LoggerConfiguration RollingAzureBlobSink(this LoggerSinkConfiguration loggerConfiguration,
                                                               ITextFormatter formatter,
                                                               CloudStorageAccount storageAccount,
                                                               string containerName,
                                                               string baseBlobName,
                                                               int batchSizeLimit,
                                                               TimeSpan period)
        {
            return loggerConfiguration.Sink(
                new RollingAzureBlobSink(formatter, storageAccount, containerName, baseBlobName, batchSizeLimit,
                                         period));
        }
    }
}