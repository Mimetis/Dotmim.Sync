//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;
//using Windows.UI.Xaml;
//using Windows.UI.Xaml.Data;

//namespace UWPSyncSample.Converters
//{
//    public class BooleanVisibilityConverter : IValueConverter
//    {
//        public object Convert(object value, Type targetType, object parameter, string language)
//        {
//            bool b = (bool)value;
//            return b ? Visibility.Visible : Visibility.Collapsed;
//        }

//        public object ConvertBack(object value, Type targetType, object parameter, string language)
//        {
//            throw new NotImplementedException();
//        }
//    }

//    public class NotBooleanVisibilityConverter : IValueConverter
//    {
//        public object Convert(object value, Type targetType, object parameter, string language)
//        {
//            bool b = (bool)value;
//            return b ? Visibility.Collapsed : Visibility.Visible;

//        }
//        public object ConvertBack(object value, Type targetType, object parameter, string language)
//        {
//            throw new NotImplementedException();
//        }
//    }
//}
