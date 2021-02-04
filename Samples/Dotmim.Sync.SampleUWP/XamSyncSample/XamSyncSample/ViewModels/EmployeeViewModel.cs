using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;
using Xamarin.Essentials;
using Xamarin.Forms;
using XamSyncSample.Models;
using XamSyncSample.Services;

namespace XamSyncSample.ViewModels
{
    [QueryProperty(nameof(RequestId), nameof(RequestId))]
    public class EmployeeViewModel : BaseViewModel
    {
        private Guid employeeId;
        private string requestId;
        private string firstName;
        private string lastName;
        private string phoneNumber;
        private string comments;
        private string photoPath;
        private string profilePictureFileName;
        private byte[] profilePicture;
        private DateTime hireDate;

        public IImageResizer ImageResizer => DependencyService.Get<IImageResizer>();

        public Command SaveCommand { get; }
        public Command TakePhotoCommand { get; }
        public Command CancelCommand { get; }
        public EmployeeViewModel()
        {
            this.SaveCommand = new Command(OnSave, ValidateSave);
            this.CancelCommand = new Command(OnCancel);
            this.TakePhotoCommand = new Command(async () => await OnTakePhotoAsync());

            this.photoPath = "DetailsPlaceholder.jpg";
            this.employeeId = Guid.NewGuid();

            // everytime check if we can save
            this.PropertyChanged += (_, __) => SaveCommand.ChangeCanExecute();
        }

        public bool IsNew { get; set; } = true;

        public string RequestId
        {
            get
            {
                return requestId;
            }
            set
            {
                requestId = value;
                LoadItemId(new Guid(value));
            }
        }
        private bool ValidateSave()
        {
            return !String.IsNullOrWhiteSpace(firstName)
                && !String.IsNullOrWhiteSpace(lastName);
        }
        public string PhotoPath
        {
            get => photoPath;
            set => SetProperty(ref photoPath, value);

        }

        public Guid EmployeeId
        {
            get => employeeId;
            set => SetProperty(ref employeeId, value);

        }
        public string FirstName
        {
            get => firstName;
            set => SetProperty(ref firstName, value);
        }

        public string LastName
        {
            get => lastName;
            set => SetProperty(ref lastName, value);
        }

        public string FullName => $"{FirstName} {LastName}";

        public string PhoneNumber
        {
            get => phoneNumber;
            set => SetProperty(ref phoneNumber, value);
        }
        public string Comments
        {
            get => comments;
            set => SetProperty(ref comments, value);
        }
        public DateTime HireDate
        {
            get => hireDate;
            set => SetProperty(ref hireDate, value);
        }
        public Byte[] ProfilePicture
        {
            get => profilePicture;
            set => SetProperty(ref profilePicture, value);
        }
        public string ProfilePictureFileName
        {
            get => profilePictureFileName;
            set => SetProperty(ref profilePictureFileName, value);
        }

        public async void LoadItemId(Guid employeeId)
        {
            try
            {
                var item = await DataStore.GetEmployeeAsync(employeeId);

                this.IsNew = item == null;

                if (!IsNew)
                    await this.FillEmployeeAsync(item);
                else
                    this.EmployeeId = Guid.NewGuid();
            }
            catch (Exception)
            {
                Debug.WriteLine("Failed to Load Item");
            }
        }

        public async Task FillEmployeeAsync(Employee item)
        {
            this.EmployeeId = item.EmployeeId;
            this.FirstName = item.FirstName;
            this.LastName = item.LastName;
            this.HireDate = item.HireDate;
            this.ProfilePicture = item.ProfilePicture;
            this.ProfilePictureFileName = item.ProfilePictureFileName;
            this.Comments = item.Comments;
            this.PhoneNumber = item.PhoneNumber;
            this.PhotoPath = Path.Combine(FileSystem.CacheDirectory, ProfilePictureFileName);

            if (!File.Exists(this.PhotoPath))
            {
                using (var writeStream = File.OpenWrite(PhotoPath))
                {
                    byte[] buffer = new byte[writeStream.Length];
                    await writeStream.WriteAsync(this.ProfilePicture, 0, this.ProfilePicture.Length);
                };

            }

            // no need byte picture array, since we have serialized the photo
            this.ProfilePicture = null;
        }


        private async void OnCancel()
        {
            // This will pop the current page off the navigation stack
            await Shell.Current.GoToAsync("..");
        }

        private async void OnSave()
        {
            Employee newEmployee = new Employee()
            {
                EmployeeId = EmployeeId,
                FirstName = FirstName,
                LastName = LastName,
                HireDate = HireDate,
                Comments = Comments,
                PhoneNumber = PhoneNumber,
                ProfilePictureFileName = ProfilePictureFileName
            };

            using (var readStream = File.OpenRead(PhotoPath))
            {
                byte[] buffer = new byte[readStream.Length];
                await readStream.ReadAsync(buffer, 0, buffer.Length);
                newEmployee.ProfilePicture = buffer;
            };

            if (IsNew)
                await DataStore.AddEmployeeAsync(newEmployee);
            else
                await DataStore.UpdateEmployeeAsync(newEmployee);

            // This will pop the current page off the navigation stack
            await Shell.Current.GoToAsync("..");
        }

        async Task OnTakePhotoAsync()
        {
            try
            {

                var photo = await MediaPicker.CapturePhotoAsync();
                await SavePhotoToCacheAsync(photo);
                Console.WriteLine($"CapturePhotoAsync COMPLETED: {PhotoPath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"CapturePhotoAsync THREW: {ex.Message}");
            }
        }


        async Task SavePhotoToCacheAsync(FileResult photo)
        {
            // canceled
            if (photo == null)
                return;

            // save the file into local storage
         
            var newFile = Path.Combine(FileSystem.CacheDirectory, photo.FileName);

            using (var stream = await photo.OpenReadAsync())
            {
                byte[] source = new byte[stream.Length];
                await stream.ReadAsync(source, 0, (int)stream.Length);

                var compressed = this.ImageResizer.ResizeImageWithFixedHeight(source, 88f);

                using (var newStream = File.OpenWrite(newFile))
                {
                    using (var memStream = new MemoryStream(compressed))
                    {
                        await memStream.CopyToAsync(newStream);
                    }
                }

            }

            this.ProfilePictureFileName = photo.FileName;
            this.PhotoPath = newFile;
        }
    }
}
