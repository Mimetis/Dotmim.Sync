using System.ComponentModel;
using Xamarin.Forms;
using XamSyncSample.ViewModels;

namespace XamSyncSample.Views
{
    public partial class EmployeeDetailPage : ContentPage
    {
        public EmployeeDetailPage()
        {
            InitializeComponent();
            BindingContext = new EmployeeViewModel();
        }
    }
}