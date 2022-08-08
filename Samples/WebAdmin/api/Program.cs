using Api;
using Api.Context;
using Dotmim.Sync.SqlServer;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers()
        .AddNewtonsoftJson(options =>
        {
          options.SerializerSettings.NullValueHandling = NullValueHandling.Ignore;
          options.SerializerSettings.Formatting = Formatting.Indented;
          options.SerializerSettings.Converters.Add(new StringEnumConverter());
          options.SerializerSettings.ReferenceLoopHandling = ReferenceLoopHandling.Serialize;
        });

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddSingleton<IDBHelper, DBHelper>();
builder.Services.AddDbContext<SyncLogsContext>(
        options => options.UseSqlServer("name=ConnectionStrings:SqlConnection"));


var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
  app.UseSwagger();
  app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
