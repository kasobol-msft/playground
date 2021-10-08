namespace ConsoleApp
{
    using Azure.Core.Diagnostics;
    using Azure.Storage.Files.DataLake;
    using Azure.Storage.Files.DataLake.Models;
    using CompressionAddOn;
    using FluentAssertions;
    using System;
    using System.IO;
    using System.Threading.Tasks;

    class Program
    {
        const string ConnectionString = "REDACTED";
        const long CompressionThreshold = 1 * 1024 * 1024;

        static async Task Main(string[] args)
        {
            using AzureEventSourceListener listener = AzureEventSourceListener.CreateConsoleLogger();

            var options = new DataLakeClientOptions(DataLakeClientOptions.ServiceVersion.V2020_10_02)
                .WithCompression(new DataLakeCompressionOptions { CompressionThreshold = CompressionThreshold });
            var serviceClient = new DataLakeServiceClient(ConnectionString, options);
            var fileSystemClient = serviceClient.GetFileSystemClient("test-filesystem");
            await fileSystemClient.CreateIfNotExistsAsync();
            var directoryClient = fileSystemClient.GetDirectoryClient("test-directory");
            await directoryClient.CreateIfNotExistsAsync();

            await UploadFile(directoryClient, 4 * 1024 * 1024);
            await UploadFile(directoryClient, 1024);
        }

        private static async Task UploadFile(DataLakeDirectoryClient directoryClient, long size)
        {
            Console.WriteLine($"\nStarting Upload File with size {size}");

            byte[] content = new byte[size];
            new Random().NextBytes(content);

            var fileName = Guid.NewGuid().ToString();
            var fileClient = directoryClient.GetAppendFileClient(fileName);
            using var contentStream = new MemoryStream(content);
            Console.WriteLine($"Appending to file with {content.Length} long content");
            await fileClient.AppendAsync(contentStream, true);
            
            FileDownloadInfo fileDownloadInfo = await fileClient.ReadAsync();
            Console.WriteLine($"File size {fileDownloadInfo.ContentLength}");
            using var fileContent = fileDownloadInfo.Content;
            using var downloadedContent = new MemoryStream();
            await fileContent.CopyToAsync(downloadedContent);

            if (size >= CompressionThreshold)
            {
                downloadedContent.ToArray().Should().NotEqual(content);
            }
            else
            {
                downloadedContent.ToArray().Should().Equal(content);
            }
        }
    }
}
