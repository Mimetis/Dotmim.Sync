using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UWPSyncSample.Context;
using UWPSyncSample.Helpers;
using Windows.Graphics.Imaging;
using Windows.UI.Xaml.Media.Imaging;

namespace UWPSyncSample.Models
{
    public class EmployeeModel : INotifyPropertyChanged
    {
        private readonly Employee employee;

        public event PropertyChangedEventHandler PropertyChanged;

        public void RaisePropertyChanged(string property) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(property));


        public static EmployeeModel NewEmployee()
        {
            var emp = new Employee();
            emp.EmployeeId = Guid.NewGuid();
            emp.HireDate = DateTime.Now;
            var empView = new EmployeeModel(emp);

            return empView;
        }
        public EmployeeModel(Employee employee)
        {
            this.employee = employee;
        }

        public async Task UpdatePictureAsync()
        {

            this.Picture = await ImageHelper.Current.FromArrayByteAsync(ProfilePicture);

        }

        public Employee GetEmployee()
        {
            
            
            return this.employee;
        }

        public Guid EmployeeId
        {
            get
            {
                return this.employee.EmployeeId;
            }
            set
            {
                if (this.employee.EmployeeId != value)
                {
                    this.employee.EmployeeId = value;
                    RaisePropertyChanged(nameof(EmployeeId));
                }
            }
        }

        public String FullName
        {
            get
            {
                if (string.IsNullOrEmpty(this.LastName))
                    return this.FirstName;
                else
                    return $"{this.FirstName} {this.LastName}";
            }
            set
            {
                var s = value.Trim();

                if (s.IndexOf(" ") > 0 && s.IndexOf(" ") < s.Length)
                {
                    this.FirstName = s.Split(" ")[0];
                    this.LastName = s.Split(" ")[0];
                }
                else
                {
                    this.FirstName = s;
                }
            }
        }

        public String FirstName
        {
            get
            {
                return this.employee.FirstName;
            }
            set
            {
                if (this.employee.FirstName != value)
                {
                    this.employee.FirstName = value;
                    RaisePropertyChanged(nameof(FirstName));
                    RaisePropertyChanged(nameof(FullName));
                }
            }
        }

        public String LastName
        {
            get
            {
                return this.employee.LastName;
            }
            set
            {
                if (this.employee.LastName != value)
                {
                    this.employee.LastName = value;
                    RaisePropertyChanged(nameof(LastName));
                    RaisePropertyChanged(nameof(FullName));
                }
            }
        }

        public String PhoneNumber
        {
            get
            {
                return this.employee.PhoneNumber;
            }
            set
            {
                if (this.employee.PhoneNumber != value)
                {
                    this.employee.PhoneNumber = value;
                    RaisePropertyChanged(nameof(PhoneNumber));
                }
            }
        }

        public String Comments
        {
            get
            {
                return this.employee.Comments;
            }
            set
            {
                if (this.employee.Comments != value)
                {
                    this.employee.Comments = value;
                    RaisePropertyChanged(nameof(Comments));
                }
            }
        }

        public Byte[] ProfilePicture
        {
            get
            {
                return this.employee.ProfilePicture;
            }
            set
            {
                if (this.employee.ProfilePicture != value)
                {
                    this.employee.ProfilePicture = value;
                    RaisePropertyChanged(nameof(ProfilePicture));
                }
            }
        }

        BitmapImage bitmapPicture;
        public BitmapImage Picture
        {
            get
            {
                return bitmapPicture;
            }
            set
            {
                if (this.bitmapPicture != value)
                {
                    this.bitmapPicture = value;
                    RaisePropertyChanged(nameof(Picture));
                }
            }
        }


        public DateTimeOffset HireDate
        {
            get
            {
                if (this.employee != null && this.employee.HireDate != DateTime.MinValue)
                    return new DateTimeOffset(this.employee.HireDate);

                return DateTimeOffset.Now;
            }
            set
            {
                if (this.employee.HireDate == DateTime.MinValue || this.employee.HireDate != value)
                {
                    this.employee.HireDate = value.DateTime;
                    RaisePropertyChanged(nameof(HireDate));
                }
            }
        }





    }
}
