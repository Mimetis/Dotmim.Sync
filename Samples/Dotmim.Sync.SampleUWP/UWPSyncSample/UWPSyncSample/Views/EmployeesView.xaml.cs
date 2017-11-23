using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using UWPSyncSample.Navigation;
using UWPSyncSample.ViewModels;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Navigation;

// The Blank Page item template is documented at https://go.microsoft.com/fwlink/?LinkId=234238

namespace UWPSyncSample.Views
{
    /// <summary>
    /// An empty page that can be used on its own or navigated to within a Frame.
    /// </summary>
    public sealed partial class EmployeesView : Page, IPageWithViewModel<EmployeesViewModel>
    {
        public EmployeesView()
        {
            this.InitializeComponent();
        }
        EmployeesViewModel employeesViewModel;
        public EmployeesViewModel ViewModel => employeesViewModel;

        public void SetViewModel(BaseViewModel viewModel)
        {
            this.employeesViewModel = viewModel as EmployeesViewModel;
        }

    }
}
