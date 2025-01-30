using Dotmim.Sync.Serialization;
using System.Collections.ObjectModel;

namespace Dotmim.Sync.Web.Server
{

    /// <summary>
    /// Specifies options for the Web Server.
    /// </summary>
    public class WebServerOptions
    {

        /// <summary>
        /// Gets converters used by different clients.
        /// </summary>
        public Collection<IConverter> Converters { get; }

        /// <summary>
        /// Gets the serializer factories.
        /// </summary>
        public Collection<ISerializerFactory> SerializerFactories { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="WebServerOptions"/> class.
        /// Create a new instance of options with default values.
        /// </summary>
        public WebServerOptions()
            : base()
        {
            this.Converters = [];
            this.SerializerFactories =
            [
                SerializersFactory.JsonSerializerFactory
            ];
        }
    }
}