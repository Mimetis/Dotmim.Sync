//using Microsoft.AspNetCore.Http;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Text;


// --------------------------------------------------------------------
// We can't use it until we add the MVC dependency... shall we ?
// --------------------------------------------------------------------

//namespace Dotmim.Sync.Web
//{
//    public class SyncActionResult : ActionResult
//    {
//        public SyncActionResult(Byte[] syncBytes)
//        {
//            this.SyncBytes = syncBytes ?? throw new ArgumentNullException("syncBytes");
//        }

//        public Byte[] SyncBytes { get; private set; }

//        public override void ExecuteResult(ControllerContext context)
//        {
//            if (context == null)
//                throw new ArgumentNullException("context");

//            HttpResponse response = context.HttpContext.Response;

//            response.Body.Write(this.SyncBytes, 0, this.SyncBytes.Length);
//        }
//    }
//}
