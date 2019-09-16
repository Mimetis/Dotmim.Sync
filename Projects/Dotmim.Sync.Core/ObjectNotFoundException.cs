namespace System.Data
{
    sealed public class ObjectNotFoundException : DataException
    {
        public ObjectNotFoundException()
        {

        }

        public ObjectNotFoundException(string message) :
            base(message)
        {

        }

        public ObjectNotFoundException(string message, Exception innerException) :
            base(message, innerException)
        {
        }

    }
}
