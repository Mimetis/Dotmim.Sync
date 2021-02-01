using System.ComponentModel;
using Xamarin.Forms;
using XamSyncSample.ViewModels;

namespace XamSyncSample.Views
{
    public partial class ItemDetailPage : ContentPage
    {
        public ItemDetailPage()
        {
            InitializeComponent();
            BindingContext = new ItemDetailViewModel();
        }
    }
}