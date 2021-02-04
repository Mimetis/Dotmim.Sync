using System;
using System.Collections.Generic;
using System.ComponentModel;
using Xamarin.Forms;
using Xamarin.Forms.Xaml;

using XamSyncSample.Models;
using XamSyncSample.ViewModels;

namespace XamSyncSample.Views
{
    public partial class NewEmployeePage : ContentPage
    {
        public NewEmployeePage()
        {
            InitializeComponent();
            BindingContext = new EmployeeViewModel();
        }
    }
}