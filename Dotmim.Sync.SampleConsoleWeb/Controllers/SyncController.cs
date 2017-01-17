using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using DmBinaryFormatter;
using Dotmim.Sync.Data.Surrogate;
using System.Net.Http;
using System.Net;
using Microsoft.Net.Http.Headers;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Dotmim.Sync.SampleConsole.Controllers
{
    [Route("api/[controller]")]
    public class SyncController : Controller
    {
        // POST api/values
        [HttpPost]
        public IActionResult Post()
        {

            var streamArray = this.Request.Body;

            DmSerializer serializer = new DmSerializer();

            var ds = serializer.Deserialize<DmSetSurrogate>(streamArray);
            var newDs = ds.ConvertToDmSet();

            newDs.DmSetName = "FromServer";

            var byteArray = serializer.Serialize(new DmSetSurrogate(newDs));

            FileContentResult contentResult = new FileContentResult(byteArray, "application/octet-stream");
            return contentResult;
        }

     
    }
}
