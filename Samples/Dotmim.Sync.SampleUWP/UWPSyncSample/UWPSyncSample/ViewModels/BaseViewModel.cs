using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Windows.UI.Xaml.Navigation;

namespace UWPSyncSample.ViewModels
{
    public abstract class BaseViewModel : INotifyPropertyChanged
    {

        public event PropertyChangedEventHandler PropertyChanged;
        protected void RaisePropertyChanged(string name) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));


        public virtual async Task Navigated(NavigationEventArgs e, CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
        }
        public virtual async Task Navigating(NavigatingCancelEventArgs e)
        {
            await Task.CompletedTask;
        }
    }
}
