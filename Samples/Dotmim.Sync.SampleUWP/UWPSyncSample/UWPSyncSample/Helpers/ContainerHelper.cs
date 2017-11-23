using Autofac;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UWPSyncSample.Context;
using UWPSyncSample.Navigation;
using UWPSyncSample.Services;
using UWPSyncSample.ViewModels;
using Windows.UI.Xaml.Controls;

namespace UWPSyncSample.Helpers
{

    /// <summary>
    /// Class helpe for managing DI 
    /// </summary>
    public class ContainerHelper
    {
        private static ContainerHelper instance;

        public static ContainerHelper Current = instance ?? (instance = new ContainerHelper());

        /// <summary>
        /// Association between a view and a view model
        /// </summary>
        private Dictionary<Type, (Type, Action<object, object, object>)> dictionary = new Dictionary<Type, (Type, Action<object, object, object>)>();


        private Dictionary<Type, Type> associatedViewModelTypes = new Dictionary<Type, Type>();


        /// <summary>
        /// Access to the underlying autofac builder
        /// </summary>
        //public ContainerBuilder Builder { get; }

        /// <summary>
        /// Get the container
        /// </summary>
        public IContainer Container { get; set; }

        public ContainerHelper()
        {
        }


        /// <summary>
        /// Register all types we need in the application
        /// </summary>
        /// <param name="frame"></param>
        public void RegisterTypes(Frame frame)
        {
            var builder = new ContainerBuilder();

            // to be able to use it in the navigation service
            builder.RegisterInstance(frame);

            // Register a Navigation Service single instance
            builder.RegisterType<NavigationService>().As<INavigationService>().SingleInstance();

            builder.RegisterType<SettingsHelper>();

            builder.Register(
                (c, p) =>
                {
                    // ge the type
                    var contosoType = p.Named<ConnectionType>("contosoType");
                    var settingsHelper = c.Resolve<SettingsHelper>();

                    // return correct contoso service
                    return new SyncHelper(contosoType, settingsHelper);
                });


            // Register my contoservices
            builder.Register<IContosoServices>(
                (c, p) =>
                {
                    // ge the type
                    var contosoType = p.Named<ConnectionType>("contosoType");
                    // retrieve correct instance
                    return new ContosoServices(contosoType);
                });


            builder.Register(
                (c, p) =>
                {
                    // ge the type
                    var contosoType = p.Named<ConnectionType>("contosoType");

                    var contosoServices = c.Resolve<IContosoServices>(new NamedParameter("contosoType", contosoType));
                    var navigationServices = c.Resolve<INavigationService>();
                    var syncHelper = c.Resolve<SyncHelper>(new NamedParameter("contosoType", contosoType));

                    return new EmployeesViewModel(navigationServices, contosoServices, syncHelper);
                });

            builder.Register(
                (c, p) =>
                {
                    // ge the type
                    var contosoType = p.Named<ConnectionType>("contosoType");

                    var contosoServices = c.Resolve<IContosoServices>(new NamedParameter("contosoType", contosoType));
                    var navigationServices = c.Resolve<INavigationService>();

                    return new EmployeeViewModel(navigationServices, contosoServices);
                });

            builder.RegisterType<SettingsViewModel>();

            this.Container = builder.Build();
        }

        /// <summary>
        /// Get the ViewModel associated with the current page type
        /// </summary>
        public BaseViewModel GetPageViewModel(Type sourcePageType, ConnectionType contosoType)
        {
            Type viewModelType = null;

            // Since we have reflection to get correct interfaces, using a dictionary
            // could be a good choice to get better performances
            if (associatedViewModelTypes.ContainsKey(sourcePageType))
            {
                viewModelType = associatedViewModelTypes[sourcePageType];
            }
            else
            {
                //   getting the ViewModel used in the page
                //each page implements IPageWithViewModel<ViewModel>

                var currentAssembly = typeof(BaseViewModel).Assembly;

                var interfaces = sourcePageType.GetInterfaces()
                                    .Where(i => i.IsGenericType && i.Assembly == currentAssembly).ToList();

                if (interfaces == null || interfaces.Count <= 0)
                    return null;

                var typeVmArray = interfaces[0].GenericTypeArguments;

                if (typeVmArray == null || typeVmArray.Length <= 0)
                    return null;

                viewModelType = typeVmArray[0];

                associatedViewModelTypes.Add(sourcePageType, viewModelType);
            }

            if (viewModelType == null)
                return null;

            var t = this.Container
                .Resolve<IContosoServices>(new NamedParameter("contosoType", contosoType));


            // resolving with a contoso type parameter
            var viewModelNavigable = this.Container
                .Resolve(viewModelType, new NamedParameter("contosoType", contosoType));

            return viewModelNavigable as BaseViewModel;

        }



        /// <summary>
        /// Can we make something easier ? :)
        /// </summary>
        internal (Type PageType, Action<object, object, object> Refresh) GetViewModelType(Type sourcePageType)
        {
            (Type, Action<object, object, object>) vmType;

            if (this.dictionary.TryGetValue(sourcePageType, out vmType))
                return vmType;

            return vmType;
        }
    }

}
