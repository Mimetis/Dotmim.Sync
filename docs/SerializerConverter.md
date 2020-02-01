
# Converters and Serializers

When using the **HTTP** mode, you will use:
- A **serializer**, to transform a database row to a serialized stream. Default is **JSON**
- A **converter**, to converter a data type to another (for example a `byte[]` array to base64 `string`). There is no default converter.

## Custom Serializer

Client dictates the serialization mechanism:

The client sends a special HTTP header `dotmim-sync-serialization-format`, containing two information:

- First one is specifying the serialization format to use. The server then knows how to deserialize the messages and also uses this serialization format in each response
- Second one is specifying if the client needs batch mode or not.

Example:

```
dotmim-sync-serialization-format: {"f":"json","s":500}
```
- Serialization : **Json**
- Batch size : **500**


### Options to set a custom MessagePack serializer

You can now set your own serializer. 
Using the `SyncOptions` instance, the client can define the serializer he wants to use.

The server shoud have the serializer installed as well.

To be able to use a new serializer, you should:
- Implements thes interfaces `ISerializerFactory` and `ISerializer<T>`
- References this serializer on both side (client and server)

Here is an example using a new serializer based on **MessagePack**, using the package [MessagePack-CSharp](https://github.com/neuecc/MessagePack-CSharp):


``` csharp
public class CustomMessagePackSerializerFactory : ISerializerFactory
{
    public string Key => "mpack";
    public ISerializer<T> GetSerializer<T>() => new CustomMessagePackSerializer<T>();
}

public class CustomMessagePackSerializer<T> : ISerializer<T>
{
    public CustomMessagePackSerializer() => MessagePackSerializer.SetDefaultResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);
    public T Deserialize(Stream ms) => MessagePackSerializer.Deserialize<T>(ms);
    public byte[] Serialize(T obj) => MessagePackSerializer.Serialize(obj);
}
```
Add the serializer to the web server serializers collection:
``` csharp
var webServerOptions = new WebServerOptions();
webServerOptions.Serializers.Add(new CustomMessagePackSerializerFactory());

```

Add the serializer on the client side:
``` csharp
var proxyClientProvider = new WebClientOrchestrator
{
    SerializerFactory = new CustomMessagePackSerializerFactory()
};
```


## Custom converter

Allows you to convert each row **before being serialized** (when send a request) and **after being deserialized** (when receiving a response)

Like the `ISerializerFactory`, you can implement your own `IConverter`.
- This convert should be available both on the client and the server.
- The server registers all converters used by any client
- The client register its own converter.

``` csharp
public interface IConverter
{

    /// <summary>
    /// get the unique key for this converter
    /// </summary>
    string Key { get; }

    /// <summary>
    /// Convert a row before being serialized
    /// </summary>
    void BeforeSerialize(SyncRow row);

    /// <summary>
    /// Convert a row afeter being deserialized
    /// </summary>
    void AfterDeserialized(SyncRow row);
}
```


Example of a simple `IConverter`:

``` csharp
    public class CustomConverter : IConverter
    {
        public string Key => "cuscom";

        public void BeforeSerialize(SyncRow row)
        {
            // Each row belongs to a Table with its own Schema
            // Easy to filter if needed
            if (row.Table.TableName != "Product")
                return;

            // Encode a specific column, named "ThumbNailPhoto"
            if (row["ThumbNailPhoto"] != null)
                row["ThumbNailPhoto"] = Convert.ToBase64String((byte[])row["ThumbNailPhoto"]);

            // Convert all DateTime columns to ticks
            foreach (var col in row.Table.Columns.Where(c => c.GetDataType() == typeof(DateTime)))
            {
                if (row[col.ColumnName] != null)
                    row[col.ColumnName] = ((DateTime)row[col.ColumnName]).Ticks;
            }
        }

        public void AfterDeserialized(SyncRow row)
        {
            // Only convert for table Product
            if (row.Table.TableName != "Product")
                return;

            // Decode photo
            row["ThumbNailPhoto"] = Convert.FromBase64String((string)row["ThumbNailPhoto"]);

            // Convert all DateTime back from ticks
            foreach (var col in row.Table.Columns.Where(c => c.GetDataType() == typeof(DateTime)))
            {
                if (row[col.ColumnName] != null)
                    row[col.ColumnName] = new DateTime(Convert.ToInt64(row[col.ColumnName]));
            }
        }
    }
```

On client side, register this converter from your `WebClientOrchestrator`:

``` csharp
// Create the web proxy client provider with specific options
var proxyClientProvider = new WebClientOrchestrator
{
    SerializerFactory = new CustomMessagePackSerializerFactory(),
    Converter = new CustomConverter()
};

```

On server side, add this converter to the list of available converters:

``` csharp
var webServerOptions = new WebServerOptions
{
   ...
};
webServerOptions.Serializers.Add(new CustomMessagePackSerializerFactory());
webServerOptions.Converters.Add(new CustomConverter());

```

Without Converter:

![image](https://user-images.githubusercontent.com/4592555/71679905-fb1ceb00-2d88-11ea-94d7-5159f5e1b8bf.png)

With Converter:

![image](https://user-images.githubusercontent.com/4592555/71679951-1be54080-2d89-11ea-96e5-a95ca891ade0.png)


