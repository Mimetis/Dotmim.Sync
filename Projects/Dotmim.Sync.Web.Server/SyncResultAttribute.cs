#if NETSTANDARD
using Microsoft.AspNetCore.Mvc.Filters;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Server
{
    public class SyncResultAttribute : ResultFilterAttribute, IResultFilter, IAsyncResultFilter
    {
        /// <summary>
        /// Methods trig synchronisation
        /// </summary>
        public string[] OnMethods { get; set; } = new[] { "*" };

        public SyncResultAttribute()
        {

        }

        /// <summary>
        /// Initialise methods trig sync, separe by comma
        /// Sample : POST,GET,DELETE
        /// </summary>
        /// <param name="methods">String represent the methods trig synchronisation</param>
        public SyncResultAttribute(string methods)
        {
            OnMethods = methods.Replace(" ", "").Split(',');
        }

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
            if (OnMethods.Contains(context.HttpContext.Request.Method)
                || OnMethods.Contains("*"))
            {
                WebProxyServerProvider _webProxyService = (WebProxyServerProvider)context.HttpContext.RequestServices.GetService(typeof(WebProxyServerProvider));

                if (_webProxyService == null)
                {
                    throw new ArgumentNullException("Proxy service not found");
                }

                await _webProxyService.HandleRequestAsync(context.HttpContext);
            }
        }
    }
}
#endif
