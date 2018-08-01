#if NETSTANDARD
using Microsoft.AspNetCore.Mvc.Filters;
#else
using System.Web.Http.Filters;
using System.Web;
#endif
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.Controllers;

namespace Dotmim.Sync.Web
{
#if NETSTANDARD
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
#else
    public class SyncResultAttribute : ActionFilterAttribute
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

        public override async Task OnActionExecutedAsync(HttpActionExecutedContext actionExecutedContext, CancellationToken cancellationToken)
        {
            bool wasSuccessful = actionExecutedContext.Exception == null;
            if (!wasSuccessful)
                return;

            await SyncContextAsync(actionExecutedContext.ActionContext);

            await base.OnActionExecutedAsync(actionExecutedContext, cancellationToken);
        }

        public override async Task OnActionExecutingAsync(HttpActionContext actionContext, CancellationToken cancellationToken)
        {
            await SyncContextAsync(actionContext);

            await base.OnActionExecutingAsync(actionContext, cancellationToken);
        }

        private async Task SyncContextAsync(HttpActionContext context)
        {
            if (OnMethods.Contains(context.Request.Method.Method)
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
#endif
}
