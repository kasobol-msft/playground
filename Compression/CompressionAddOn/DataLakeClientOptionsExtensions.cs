using Azure.Storage.Files.DataLake;

namespace CompressionAddOn
{
    public static class DataLakeClientOptionsExtensions
    {
        public static DataLakeClientOptions WithCompression(this DataLakeClientOptions options, DataLakeCompressionOptions compressionOptions = default)
        {
            options.AddPolicy(new CompressingPolicy(compressionOptions), Azure.Core.HttpPipelinePosition.PerCall);
            return options;
        }
    }
}
