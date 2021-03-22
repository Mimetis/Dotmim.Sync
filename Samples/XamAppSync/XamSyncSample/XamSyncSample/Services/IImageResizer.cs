using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;

namespace XamSyncSample.Services
{
    public interface IImageResizer
    {
        byte[] ResizeImage(byte[] imageData, float width, float height);
        byte[] ResizeImage(byte[] imageData, int compressionPercentage);
        byte[] ResizeImageWithFixedHeight(byte[] imageData, float height);
        byte[] ResizeImageWithFixedWidth(byte[] imageData, float width);
    }
}
