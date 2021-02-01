using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XamSyncSample.Helpers
{
    public class ImageHelper
    {



        private static ImageHelper current;

        public static ImageHelper Current
        {
            get
            {
                if (current == null)
                    current = new ImageHelper();

                return current;
            }
        }

        //public async Task<Byte[]> ToByteArrayAsync(StorageFile bitmap)
        //{
        //    using (var stream = await bitmap.OpenSequentialReadAsync())
        //    {
        //        var readStream = stream.AsStreamForRead();
        //        byte[] buffer = new byte[readStream.Length];
        //        await readStream.ReadAsync(buffer, 0, buffer.Length);
        //        return buffer;
        //    }
        //}

        //public async Task<BitmapImage> FromArrayByteAsync(Byte[] imageArray)
        //{
        //    if (imageArray == null || imageArray.Length <= 0)
        //        return null;

        //    BitmapImage bitmapImage = new BitmapImage();

        //    using (InMemoryRandomAccessStream stream = new InMemoryRandomAccessStream())
        //    {
        //        using (DataWriter writer = new DataWriter(stream.GetOutputStreamAt(0)))
        //        {
        //            writer.WriteBytes(imageArray);
        //            await writer.StoreAsync();
        //        }
        //        BitmapImage image = new BitmapImage();
        //        await image.SetSourceAsync(stream);
        //        return image;
        //    }
        //}

       
        //public static async Task<BitmapImage> SaveImageToCacheAndGetImage(Byte[] imageArray, string fileName)
        //{
        //    var folder = ApplicationData.Current.LocalFolder;
        //    var file = await folder.CreateFileAsync(fileName, CreationCollisionOption.ReplaceExisting);
        //    var bitmapImage = new BitmapImage();

        //    try
        //    {
        //        using (MemoryStream memoryStream = new MemoryStream(imageArray))
        //        {
        //            using (var randomAccessStream = memoryStream.AsRandomAccessStream())
        //            {
        //                BitmapDecoder decoder = await BitmapDecoder.CreateAsync(randomAccessStream);

        //                uint width = 80;
        //                var height = decoder.OrientedPixelHeight * width / decoder.OrientedPixelWidth;

        //                using (var stream = await file.OpenAsync(FileAccessMode.ReadWrite))
        //                {
        //                    BitmapEncoder encoder = await BitmapEncoder.CreateForTranscodingAsync(stream, decoder);

        //                    encoder.BitmapTransform.ScaledHeight = height;
        //                    encoder.BitmapTransform.ScaledWidth = width;

        //                    await encoder.FlushAsync();

        //                    stream.Seek(0);
        //                    await bitmapImage.SetSourceAsync(stream);
        //                }
        //            }

        //        }
        //        return bitmapImage;

        //    }
        //    catch (Exception)
        //    {

        //    }

        //    return null;

        //}

   

        //public async Task<BitmapImage> SaveImageToCacheAndGetImage2(Byte[] imageArray, string fileName)
        //{
        //    try
        //    {
        //        var folder = ApplicationData.Current.LocalFolder;

        //        var file = await folder.CreateFileAsync(fileName, CreationCollisionOption.ReplaceExisting);

        //        var bitmapImage = new BitmapImage();

        //        using (var stream = await file.OpenAsync(FileAccessMode.ReadWrite))
        //        {
        //            await stream.WriteAsync(imageArray.AsBuffer());

        //            stream.Seek(0);
        //            await bitmapImage.SetSourceAsync(stream);
        //            stream.Dispose();
        //        }


        //        return bitmapImage;
        //    }
        //    catch (Exception ex)
        //    {
        //        Debug.WriteLine(ex.Message);
        //        return null;
        //    }
        //}

        //public async Task<(BitmapImage Image, Uri uri)> GetImageFromCacheAsync(string fileName)
        //{
        //    try
        //    {
        //        var folder = ApplicationData.Current.LocalFolder;

        //        var file = await folder.TryGetItemAsync(fileName) as StorageFile;

        //        if (file == null)
        //            return (null, null);

        //        BitmapImage bitmapImage = new BitmapImage();
        //        using (var stream = await file.OpenAsync(FileAccessMode.Read))
        //        {
        //            await bitmapImage.SetSourceAsync(stream);
        //        }

        //        var imgUri = new Uri("ms-appdata:///local/" + fileName);

        //        // bitmapImage.UriSource = new Uri(file.Path);
        //        return (bitmapImage, imgUri);
        //    }
        //    catch (Exception ex)
        //    {
        //        Debug.WriteLine(ex.Message);
        //        return (null, null);
        //    }

        //}


    }
}
