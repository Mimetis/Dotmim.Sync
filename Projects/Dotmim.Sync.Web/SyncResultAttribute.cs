using Microsoft.AspNetCore.Mvc.Filters;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web
{
    public class SyncResultAttribute : ResultFilterAttribute, IResultFilter, IAsyncResultFilter
    {
        public string[] OnMethods { get; set; } = new[] { "POST" };

        public override async void OnResultExecuted(ResultExecutedContext context)
        {
            base.OnResultExecuted(context);

            await SyncContextAsync(context);
        }

        public override async Task OnResultExecutionAsync(ResultExecutingContext context, ResultExecutionDelegate next)
        {
            await SyncContextAsync(context);

            await next();
        }

        private async Task SyncContextAsync(FilterContext context)
        {
            if (OnMethods.Contains(context.HttpContext.Request.Method))
            {
                WebProxyServerProvider _webProxyService = (WebProxyServerProvider)context.HttpContext.RequestServices.GetService(typeof(WebProxyServerProvider));

                if (_webProxyService == null) { 
                    throw new ArgumentNullException("Proxy service not found");
                }

                await _webProxyService.HandleRequestAsync(context.HttpContext);
            }
        }
    }
}
