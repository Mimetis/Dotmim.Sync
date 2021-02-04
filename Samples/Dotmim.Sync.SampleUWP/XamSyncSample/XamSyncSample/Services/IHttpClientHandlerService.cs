using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;

namespace XamSyncSample.Services
{
    public interface IHttpClientHandlerService
    {
        HttpClientHandler GetInsecureHandler();
    }
}
