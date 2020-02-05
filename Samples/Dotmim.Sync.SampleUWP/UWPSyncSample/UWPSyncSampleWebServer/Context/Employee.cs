using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UWPSyncSampleWebServer.Context
{
    public class Employee
    {
        public Guid EmployeeId { get; set; }
        public String FirstName { get; set; }
        public String LastName { get; set; }
        public Byte[] ProfilePicture { get; set; }
        public String PhoneNumber { get; set; }
        public DateTime HireDate { get; set; }
        public String Comments { get; set; }
    }
}
