//using System;
//using System.Collections.Generic;
//using System.Net;
//using System.Net.Http;
//using System.Text;

//namespace Dotmim.Sync.Web
//{
//    public class WebSyncException : HttpRequestException
//    {
//        public WebSyncException(string message) : base(message)
//        {
//        }
    
//        public SyncExceptionType Type { get; set; }
//        /// <summary>
//        /// Get message string from Exception
//        /// </summary>
//        internal static WebSyncException GetWebSyncException(Exception exception)
//        {
//            WebSyncException webSyncException = new WebSyncException(exception.Message);

//            SyncException syncException = exception as SyncException;

//            webSyncException.Type = syncException != null ? syncException.Type : SyncExceptionType.Unknown;
           
//            return webSyncException;
//        }
//    }
//}
