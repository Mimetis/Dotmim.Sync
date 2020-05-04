// ******************************************************************
// Copyright (c) Microsoft. All rights reserved.
// This code is licensed under the MIT License (MIT).
// THE CODE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
// INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
// TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH
// THE CODE OR THE USE OR OTHER DEALINGS IN THE CODE.
// ******************************************************************

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using Microsoft.Toolkit.Uwp.Helpers;
using UWPSyncSample.Context;
using UWPSyncSample.Helpers;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Navigation;

namespace UWPSyncSample.Navigation
{
    public class NavigationService : INavigationService
    {
        private bool isNavigating;
        private CancellationTokenSource tokenSource;
      
        public ConnectionType ContosoType { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="NavigationService"/> class.
        /// </summary>
        public NavigationService(Frame frame)
        {
            Frame = frame;

            // Register events
            Frame.Navigated += Frame_Navigated;
            Frame.Navigating += Frame_Navigating;
            Frame.NavigationFailed += (s, e) => throw new Exception("Failed to load Page " + e.SourcePageType.FullName);

        }

        
        public event EventHandler<NavigationEventArgs> Navigated;

        public bool CanGoBack => Frame.CanGoBack;

        /// <summary>
        /// Gets the main page IFrame
        /// </summary>
        private Frame Frame { get; }

        ///// <summary>
        ///// Graphprovider, 
        ///// </summary>
        //public IGraphProvider GraphProvider { get; }

        /// <summary>
        /// Gets or Sets a value indicating if we are currently navigating
        /// </summary>
        public bool IsNavigating
        {
            get => isNavigating;

            set
            {
                if (value != isNavigating)
                {
                    isNavigating = value;
                }
            }
        }


        /// <summary>
        /// Navigate in the back direction
        /// </summary>
        /// <returns>A task that can be awaited</returns>
        public async Task GoBackAsync()
        {
            if (Frame.CanGoBack)
            {
                IsNavigating = true;

                if (tokenSource != null)
                    tokenSource.Cancel();

                await DispatcherHelper.ExecuteOnUIThreadAsync(() => Frame.GoBack());
            }
        }

        /// <summary>
        /// The Navigating event. Occurs before Page.OnNavigatingFrom
        /// </summary>
        private async void Frame_Navigating(object sender, NavigatingCancelEventArgs e)
        {
            IsNavigating = !e.Cancel;
            await Task.CompletedTask;
        }


        /// <summary>
        /// The Navigated event. This event is raised BEFORE Page.OnNavigatedTo 
        /// </summary>
        private async void Frame_Navigated(object sender, NavigationEventArgs e)
        {
            IPageWithViewModel pageWithViewModel = e.Content as IPageWithViewModel;

            if (pageWithViewModel == null)
                return;

            // Get the view model associated with the current page
            var viewModelNavigable = ContainerHelper.Current.GetPageViewModel(e.SourcePageType, this.ContosoType);

            if (viewModelNavigable != null)
            {
                tokenSource = new CancellationTokenSource();
                pageWithViewModel.SetViewModel(viewModelNavigable);

                // Check event
                Navigated?.Invoke(this, e);

                await viewModelNavigable.Navigated(e, tokenSource.Token);
            }


            IsNavigating = false;

        }


        /// <summary>
        /// Navigate to a page without parameter
        /// </summary>
        public Task NavigateToPage<TPage>()
        {
            return NavigateToPage<TPage>(this.ContosoType);
        }

        /// <summary>
        /// Navigate to a page with a parameter
        /// </summary>
        public Task NavigateToPage<TPage>(object parameter)
        {
            return NavigateToPage<TPage>(this.ContosoType, parameter);
        }

        /// <summary>
        /// Navigate to a page with a parameter
        /// </summary>
        public async Task NavigateToPage<TPage>(ConnectionType contosoType, object parameter  = null)
        {
            // Early out if already in the middle of a Navigation
            if (isNavigating && tokenSource != null)
                tokenSource.Cancel();

            this.ContosoType = contosoType;
            this.isNavigating = true;

            await DispatcherHelper.ExecuteOnUIThreadAsync(() =>
                    Frame.Navigate(typeof(TPage), parameter));
        }

    }
}
