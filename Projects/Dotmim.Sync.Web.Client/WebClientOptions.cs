//using Dotmim.Sync.Serialization;
//using System;
//using System.Collections.Generic;
//using System.Net;
//using System.Net.Http;
//using System.Text;

//namespace Dotmim.Sync.Web.Client
//{

//    /// <summary>
//    /// Specific options when used in a web api mode
//    /// </summary>
//    public class WebClientOptions
//    {
//        /// <summary>
//        /// Gets or Sets the serializer used by the client. Default is Json
//        /// </summary>
//        public ISerializerFactory SerializerFactory { get; set; }

//        /// <summary>
//        /// Gets or Sets the request handler used by the http client
//        /// </summary>
//        public HttpClientHandler HttpClientHandler { get; set; }

//        /// <summary>
//        /// Create a new instance of specific web client orchestrator options
//        /// </summary>
//        public WebClientOptions()
//        {
//            this.SerializerFactory = SerializersCollection.JsonSerializer;
//            this.HttpClientHandler = new HttpClientHandler();


//        }

//    }
//}
