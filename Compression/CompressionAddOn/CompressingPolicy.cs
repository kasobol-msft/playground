namespace CompressionAddOn
{
    using Azure.Core;
    using Azure.Core.Pipeline;
    using System;
    using System.Globalization;
    using System.IO;
    using System.IO.Compression;
    using System.Threading.Tasks;

    internal class CompressingPolicy : HttpPipelinePolicy
    {
        private readonly long _compressionThreshold;

        public CompressingPolicy(DataLakeCompressionOptions options)
        {
            _compressionThreshold = options?.CompressionThreshold ?? 0;
        }

        public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            // TODO this should be implemented as well using sync stream apis.
            throw new NotImplementedException();
        }

        public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            var uri = message.Request.Uri;
            if (uri.Query.Contains("action=concurrentAppend"))
            {
                Console.WriteLine("Intercepting concurrent append");
                var requestContent = message.Request.Content;

                message.Request.Content.TryComputeLength(out var originalSize);
                Console.WriteLine($"Original content size {originalSize}");

                if (originalSize >= _compressionThreshold)
                {
                    Console.WriteLine("Compressing data");
                    var memoryStream = new MemoryStream();
                    var compressedStream = new GZipStream(memoryStream, CompressionMode.Compress);
                    await requestContent.WriteToAsync(compressedStream, message.CancellationToken);
                    memoryStream.Position = 0;
                    Console.WriteLine($"Compressed content size {memoryStream.Length}");

                    var newRequestContent = RequestContent.Create(memoryStream);
                    message.Request.Content = newRequestContent;
                    message.Request.Headers.SetValue("Content-Length", memoryStream.Length.ToString(CultureInfo.InvariantCulture));
                    message.Request.Headers.SetValue("x-ms-original-content-length", originalSize.ToString(CultureInfo.InvariantCulture));
                    message.Request.Headers.SetValue("x-ms-compressed-chunk-count", "1");
                }
                else
                {
                    Console.WriteLine($"Not compressing data smaller than {_compressionThreshold}");
                }
            }

            await ProcessNextAsync(message, pipeline);
        }
    }
}
