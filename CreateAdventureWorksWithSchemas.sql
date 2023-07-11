USE [master]
GO 

-- Server database
if (exists (select * from sys.databases where name = 'AdventureWorks'))
Begin
	ALTER DATABASE [AdventureWorks] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE [AdventureWorks]
End
Create database [AdventureWorks]
Go
-- Client database. No need to create the schema, Dotmim.Sync will do
if (exists (select * from sys.databases where name = 'Client'))
Begin
	ALTER DATABASE [Client] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE [Client]
End
Create database [Client]
Go
---- Client database. No need to create the schema, Dotmim.Sync will do
--if (exists (select * from sys.databases where name = 'Client2'))
--Begin
--	ALTER DATABASE [Client2] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE;
--	DROP DATABASE [Client2]
--End
--Create database [Client2]

/*
ALTER DATABASE [AdventureWorks] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)
ALTER DATABASE [Client] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)
*/

GO

USE [AdventureWorks]
GO
/****** Object:  Schema [SalesLT]    Script Date: 11/07/2023 21:19:21 ******/
CREATE SCHEMA [SalesLT]
GO
/****** Object:  Table [dbo].[Address]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Address](
	[AddressID] [int] IDENTITY(1,1) NOT NULL,
	[AddressLine1] [nvarchar](max) NOT NULL,
	[AddressLine2] [nvarchar](max) NULL,
	[City] [nvarchar](30) NULL,
	[StateProvince] [nvarchar](50) NULL,
	[CountryRegion] [nvarchar](50) NULL,
	[PostalCode] [nvarchar](15) NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime2](7) NULL,
 CONSTRAINT [PK_Address] PRIMARY KEY CLUSTERED 
(
	[AddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Customer]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Customer](
	[CustomerID] [uniqueidentifier] NOT NULL,
	[EmployeeID] [int] NULL,
	[NameStyle] [bit] NOT NULL,
	[Title] [nvarchar](8) NULL,
	[FirstName] [nvarchar](50) NOT NULL,
	[MiddleName] [nvarchar](50) NULL,
	[LastName] [nvarchar](50) NOT NULL,
	[Suffix] [nvarchar](10) NULL,
	[CompanyName] [nvarchar](128) NULL,
	[SalesPerson] [nvarchar](256) NULL,
	[EmailAddress] [nvarchar](50) NULL,
	[Phone] [nvarchar](25) NULL,
	[PasswordHash] [varchar](128) NULL,
	[PasswordSalt] [varchar](10) NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime2](7) NULL,
	[Attribute With Space] [nvarchar](250) NULL,
 CONSTRAINT [PK_Customer] PRIMARY KEY CLUSTERED 
(
	[CustomerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CustomerAddress]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CustomerAddress](
	[CustomerID] [uniqueidentifier] NOT NULL,
	[AddressID] [int] NOT NULL,
	[AddressType] [nvarchar](50) NOT NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime2](7) NULL,
 CONSTRAINT [PK_CustomerAddress] PRIMARY KEY CLUSTERED 
(
	[CustomerID] ASC,
	[AddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Employee]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Employee](
	[EmployeeId] [int] IDENTITY(1,1) NOT NULL,
	[FirstName] [nvarchar](50) NOT NULL,
	[LastName] [nvarchar](50) NOT NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime2](7) NULL,
 CONSTRAINT [PK_Employee] PRIMARY KEY CLUSTERED 
(
	[EmployeeId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[EmployeeAddress]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EmployeeAddress](
	[EmployeeID] [int] NOT NULL,
	[AddressID] [int] NOT NULL,
	[AddressType] [nvarchar](50) NOT NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime2](7) NULL,
 CONSTRAINT [PK_EmployeeAddress] PRIMARY KEY CLUSTERED 
(
	[EmployeeID] ASC,
	[AddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Log]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Log](
	[Oid] [uniqueidentifier] NOT NULL,
	[TimeStampDate] [datetime2](7) NULL,
	[Operation] [nvarchar](50) NULL,
	[ErrorDescription] [nvarchar](50) NULL,
	[OptimisticLockField] [int] NULL,
	[GCRecord] [int] NULL,
 CONSTRAINT [PK_Log] PRIMARY KEY CLUSTERED 
(
	[Oid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Posts]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Posts](
	[PostId] [int] IDENTITY(1,1) NOT NULL,
	[Title] [nvarchar](max) NULL,
 CONSTRAINT [PK_Posts] PRIMARY KEY CLUSTERED 
(
	[PostId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PostTag]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PostTag](
	[PostId] [int] NOT NULL,
	[TagId] [int] NOT NULL,
 CONSTRAINT [PK_PostTag] PRIMARY KEY CLUSTERED 
(
	[PostId] ASC,
	[TagId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PricesList]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PricesList](
	[PriceListId] [uniqueidentifier] NOT NULL,
	[Description] [nvarchar](50) NOT NULL,
	[From] [datetime2](7) NULL,
	[To] [datetime2](7) NULL,
 CONSTRAINT [PK_PricesList] PRIMARY KEY CLUSTERED 
(
	[PriceListId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PricesListCategory]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PricesListCategory](
	[PriceListId] [uniqueidentifier] NOT NULL,
	[PriceCategoryId] [nvarchar](12) NOT NULL,
 CONSTRAINT [PK_PricesListCategory] PRIMARY KEY CLUSTERED 
(
	[PriceListId] ASC,
	[PriceCategoryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PricesListDetail]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PricesListDetail](
	[PriceListId] [uniqueidentifier] NOT NULL,
	[PriceCategoryId] [nvarchar](12) NOT NULL,
	[PriceListDettailId] [uniqueidentifier] NOT NULL,
	[ProductId] [uniqueidentifier] NOT NULL,
	[ProductDescription] [nvarchar](50) NOT NULL,
	[Amount] [decimal](18, 2) NOT NULL,
	[Discount] [decimal](18, 2) NOT NULL,
	[Total]  AS ([Amount]-[Discount]),
	[MinQuantity] [int] NULL,
 CONSTRAINT [PK_PricesListDetail] PRIMARY KEY CLUSTERED 
(
	[PriceListId] ASC,
	[PriceCategoryId] ASC,
	[PriceListDettailId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tags]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tags](
	[TagId] [int] IDENTITY(1,1) NOT NULL,
	[Text] [nvarchar](max) NULL,
 CONSTRAINT [PK_Tags] PRIMARY KEY CLUSTERED 
(
	[TagId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [SalesLT].[Product]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [SalesLT].[Product](
	[ProductID] [uniqueidentifier] NOT NULL,
	[Name] [nvarchar](50) NOT NULL,
	[ProductNumber] [nvarchar](25) NULL,
	[Color] [nvarchar](15) NULL,
	[StandardCost] [money] NULL,
	[ListPrice] [money] NULL,
	[Size] [nvarchar](5) NULL,
	[Weight] [decimal](8, 2) NULL,
	[ProductCategoryID] [nvarchar](12) NULL,
	[ProductModelID] [int] NULL,
	[SellStartDate] [datetime2](7) NULL,
	[SellEndDate] [datetime2](7) NULL,
	[DiscontinuedDate] [datetime2](7) NULL,
	[ThumbNailPhoto] [varbinary](max) NULL,
	[ThumbnailPhotoFileName] [nvarchar](50) NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime2](7) NULL,
 CONSTRAINT [PK_Product] PRIMARY KEY CLUSTERED 
(
	[ProductID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [SalesLT].[ProductCategory]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [SalesLT].[ProductCategory](
	[ProductCategoryID] [nvarchar](12) NOT NULL,
	[ParentProductCategoryId] [nvarchar](12) NULL,
	[Name] [nvarchar](50) NOT NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime2](7) NULL,
	[Attribute With Space] [nvarchar](250) NULL,
 CONSTRAINT [PK_ProductCategory] PRIMARY KEY CLUSTERED 
(
	[ProductCategoryID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [SalesLT].[ProductModel]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [SalesLT].[ProductModel](
	[ProductModelID] [int] IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](50) NOT NULL,
	[CatalogDescription] [nvarchar](max) NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime2](7) NULL,
 CONSTRAINT [PK_ProductModel] PRIMARY KEY CLUSTERED 
(
	[ProductModelID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [SalesLT].[SalesOrderDetail]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [SalesLT].[SalesOrderDetail](
	[SalesOrderDetailID] [int] IDENTITY(1,1) NOT NULL,
	[SalesOrderID] [int] NOT NULL,
	[OrderQty] [smallint] NOT NULL,
	[ProductID] [uniqueidentifier] NOT NULL,
	[UnitPrice] [money] NOT NULL,
	[UnitPriceDiscount] [money] NOT NULL,
	[LineTotal] [numeric](38, 6) NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime2](7) NULL,
 CONSTRAINT [PK_SalesOrderDetail] PRIMARY KEY CLUSTERED 
(
	[SalesOrderDetailID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [SalesLT].[SalesOrderHeader]    Script Date: 11/07/2023 21:19:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [SalesLT].[SalesOrderHeader](
	[SalesOrderID] [int] IDENTITY(1,1) NOT NULL,
	[RevisionNumber] [smallint] NOT NULL,
	[OrderDate] [datetime2](7) NULL,
	[DueDate] [datetime2](7) NULL,
	[ShipDate] [datetimeoffset](7) NULL,
	[Status] [smallint] NOT NULL,
	[OnlineOrderFlag] [bit] NOT NULL,
	[SalesOrderNumber] [nvarchar](25) NOT NULL,
	[PurchaseOrderNumber] [nvarchar](25) NULL,
	[AccountNumber] [nvarchar](15) NULL,
	[CustomerID] [uniqueidentifier] NOT NULL,
	[ShipToAddressID] [int] NULL,
	[BillToAddressID] [int] NULL,
	[ShipMethod] [nvarchar](50) NOT NULL,
	[CreditCardApprovalCode] [varchar](15) NULL,
	[SubTotal] [money] NOT NULL,
	[TaxAmt] [money] NOT NULL,
	[Freight] [money] NOT NULL,
	[TotalDue] [money] NOT NULL,
	[Comment] [nvarchar](max) NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime2](7) NULL,
 CONSTRAINT [PK_SalesOrderHeader] PRIMARY KEY CLUSTERED 
(
	[SalesOrderID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
SET IDENTITY_INSERT [dbo].[Address] ON 
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (1, N'8713 Yosemite Ct.', N'Appt 1', N'Bothell', N'Washington', N'United States', N'98011', N'8164083b-2b5f-43d2-ab02-4123bc821583', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (2, N'1318 Lasalle Street', N'Appt 2', N'Bothell', N'Washington', N'United States', N'98011', N'e9ed7efd-b6aa-4916-9f7b-cda9d9a0f579', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (3, N'9178 Jumping St.', N'Appt 3', N'Dallas', N'Texas', N'United States', N'75201', N'b9749ca5-9229-490a-bd88-9ed7129ed4c8', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (4, N'9228 Via Del Sol', N'Appt 4', N'Phoenix', N'Arizona', N'United States', N'85004', N'ae553167-85cb-4d17-82eb-d5a7f2856ab3', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (5, N'26910 Indela Road', N'Appt 5', N'Montreal', N'Quebec', N'Canada', N'H1Y 2H5', N'a942a109-48a5-4f34-a720-a10b4802e56b', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (6, N'2681 Eagle Peak', N'Appt 6', N'Bellevue', N'Washington', N'United States', N'98004', N'67bee189-e00c-46bf-a758-82e95c123966', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (7, N'7943 Walnut Ave', N'Appt 7', N'Renton', N'Washington', N'United States', N'98055', N'763238cc-2803-421c-9de4-7f566aef0bfa', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (8, N'6388 Lake City Way', N'Appt 8', N'Burnaby', N'British Columbia', N'Canada', N'V5A 3A6', N'956dbb9b-5a4a-440f-8417-7df25344b9cd', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (9, N'52560 Free Street', N'Appt 9', N'Toronto', N'Ontario', N'Canada', N'M4B 1V7', N'8dcc8e73-e0b3-4c0d-b98b-d07eee9f70ff', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (10, N'22580 Free Street', N'Appt 10', N'Toronto', N'Ontario', N'Canada', N'M4B 1V7', N'55cbef99-4d92-4234-b6ef-68033a2d4f24', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (11, N'2575 Bloor Street East', N'Appt 11', N'Toronto', N'Ontario', N'Canada', N'M4B 1V6', N'b3f1ddb9-3f78-4348-9a46-25aa53c7de8a', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (12, N'Station E', N'Appt 12', N'Chalk Riber', N'Ontario', N'Canada', N'K0J 1J0', N'46b27174-af89-405d-82e7-caae5a0d29b3', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (13, N'575 Rue St Amable', N'Appt 13', N'Quebec', N'Quebec', N'Canada', N'G1R', N'9590e78a-105d-4c8c-97ec-b45f62d96a1e', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (14, N'2512-4th Ave Sw', N'Appt 14', N'Calgary', N'Alberta', N'Canada', N'T2P 2G8', N'ecf82474-9868-4753-8dd7-494d0de3bdbc', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (15, N'55 Lakeshore Blvd East', N'Appt 15', N'Toronto', N'Ontario', N'Canada', N'M4B 1V6', N'd3ce8929-fbfd-4056-b837-2380cd9e07d8', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (16, N'6333 Cote Vertu', N'Appt 16', N'Montreal', N'Quebec', N'Canada', N'H1Y 2H5', N'24cff694-2cac-47b2-8d45-a24cf432a1c0', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (17, N'3255 Front Street West', N'Appt 17', N'Toronto', N'Ontario', N'Canada', N'H1Y 2H5', N'bd92cc03-132e-41d7-91d6-1d8fad685dca', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (18, N'2550 Signet Drive', N'Appt 18', N'Weston', N'Ontario', N'Canada', N'H1Y 2H7', N'45a1fa52-14bd-41df-9f3c-fff971731484', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (19, N'6777 Kingsway', N'Appt 19', N'Burnaby', N'British Columbia', N'Canada', N'H1Y 2H8', N'ca4df50a-c2fb-4f62-aec7-21b1c6d4c41b', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (20, N'5250-505 Burning St', N'Appt 20', N'Vancouver', N'British Columbia', N'Canada', N'H1Y 2H9', N'a2c1f5a3-033d-4bc5-b51e-65261e92cacc', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
INSERT [dbo].[Address] ([AddressID], [AddressLine1], [AddressLine2], [City], [StateProvince], [CountryRegion], [PostalCode], [rowguid], [ModifiedDate]) VALUES (21, N'600 Slater Street', N'Appt 21', N'Ottawa', N'Ontario', N'Canada', N'M9V 4W3', N'33ef43bc-d3a9-4718-ab62-bb0471e7836c', CAST(N'2023-07-11T21:17:22.7833333' AS DateTime2))
GO
SET IDENTITY_INSERT [dbo].[Address] OFF
GO
INSERT [dbo].[Customer] ([CustomerID], [EmployeeID], [NameStyle], [Title], [FirstName], [MiddleName], [LastName], [Suffix], [CompanyName], [SalesPerson], [EmailAddress], [Phone], [PasswordHash], [PasswordSalt], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'd43a86c2-1db9-4bdd-a93b-1c2118c3f5c4', 1, 0, N'Mr.', N'Keith', N'N.', N'Harris', NULL, N'Progressive Sports', N'adventure-works\david8', N'keith0@adventure-works.com', N'170-555-0127', N'YPdtRdvqeAhj6wyxEsFdshBDNXxkCXn+CRgbvJItknw=', N'fs1ZGhY=', N'b7d9e0e6-0363-4bcb-acd9-ed27a38b6fc7', CAST(N'2023-07-11T21:17:22.8200000' AS DateTime2), NULL)
GO
INSERT [dbo].[Customer] ([CustomerID], [EmployeeID], [NameStyle], [Title], [FirstName], [MiddleName], [LastName], [Suffix], [CompanyName], [SalesPerson], [EmailAddress], [Phone], [PasswordHash], [PasswordSalt], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'75939e87-cb78-4a67-b88e-28607d93b09f', 3, 0, N'Ms.', N'Janet', N'M.', N'Gates', NULL, N'Modular Cycle Systems', N'adventure-works\jillian0', N'janet1@adventure-works.com', N'710-555-0173', N'ElzTpSNbUW1Ut+L5cWlfR7MF6nBZia8WpmGaQPjLOJA=', N'nm7D5e4=', N'7a553d49-4329-47be-802c-4eff3d3de170', CAST(N'2023-07-11T21:17:22.8200000' AS DateTime2), NULL)
GO
INSERT [dbo].[Customer] ([CustomerID], [EmployeeID], [NameStyle], [Title], [FirstName], [MiddleName], [LastName], [Suffix], [CompanyName], [SalesPerson], [EmailAddress], [Phone], [PasswordHash], [PasswordSalt], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'1867e41d-23e9-4496-b84e-e0fd30088db2', 1, 0, N'Mr.', N'Orlando', N'N.', N'Gee', NULL, N'A Bike Store', N'adventure-works\pamela0', N'orlando0@adventure-works.com', N'245-555-0173', N'L/Rlwxzp4w7RWmEgXX+/A7cXaePEPcp+KwQhl2fJL7w=', N'1KjXYs4=', N'397b1fb6-71e0-450e-871d-bdee11dfd8fb', CAST(N'2023-07-11T21:17:22.8200000' AS DateTime2), NULL)
GO
INSERT [dbo].[Customer] ([CustomerID], [EmployeeID], [NameStyle], [Title], [FirstName], [MiddleName], [LastName], [Suffix], [CompanyName], [SalesPerson], [EmailAddress], [Phone], [PasswordHash], [PasswordSalt], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'4a0c5ad9-7ab6-401e-94d2-eb62b2e7a847', 2, 0, N'Ms.', N'Donna', N'F.', N'Carreras', NULL, N'Advanced Bike Components', N'adventure-works\jillian0', N'donna0@adventure-works.com', N'279-555-0130', N'LNoK27abGQo48gGue3EBV/UrlYSToV0/s87dCRV7uJk=', N'YTNH5Rw=', N'ef6b5490-1a98-4c57-8809-feeb33b6315b', CAST(N'2023-07-11T21:17:22.8200000' AS DateTime2), NULL)
GO
INSERT [dbo].[CustomerAddress] ([CustomerID], [AddressID], [AddressType], [rowguid], [ModifiedDate]) VALUES (N'd43a86c2-1db9-4bdd-a93b-1c2118c3f5c4', 3, N'Main Office', N'b003e6b5-b7b7-45ea-9a09-c05aecf53be0', CAST(N'2023-07-11T21:17:22.8500000' AS DateTime2))
GO
INSERT [dbo].[CustomerAddress] ([CustomerID], [AddressID], [AddressType], [rowguid], [ModifiedDate]) VALUES (N'75939e87-cb78-4a67-b88e-28607d93b09f', 1, N'Main Office', N'6cd0db47-b19f-4b56-849b-ad80173f5fe3', CAST(N'2023-07-11T21:17:22.8500000' AS DateTime2))
GO
INSERT [dbo].[CustomerAddress] ([CustomerID], [AddressID], [AddressType], [rowguid], [ModifiedDate]) VALUES (N'1867e41d-23e9-4496-b84e-e0fd30088db2', 4, N'Main Office', N'5057f1b8-1146-4eab-9992-81172d8dc972', CAST(N'2023-07-11T21:17:22.8500000' AS DateTime2))
GO
INSERT [dbo].[CustomerAddress] ([CustomerID], [AddressID], [AddressType], [rowguid], [ModifiedDate]) VALUES (N'1867e41d-23e9-4496-b84e-e0fd30088db2', 5, N'Office Depot', N'078b6b89-d156-42d9-ad7f-b54682153571', CAST(N'2023-07-11T21:17:22.8500000' AS DateTime2))
GO
INSERT [dbo].[CustomerAddress] ([CustomerID], [AddressID], [AddressType], [rowguid], [ModifiedDate]) VALUES (N'4a0c5ad9-7ab6-401e-94d2-eb62b2e7a847', 2, N'Main Office', N'99a6107c-98cc-4a33-a93a-22eab46af438', CAST(N'2023-07-11T21:17:22.8500000' AS DateTime2))
GO
SET IDENTITY_INSERT [dbo].[Employee] ON 
GO
INSERT [dbo].[Employee] ([EmployeeId], [FirstName], [LastName], [rowguid], [ModifiedDate]) VALUES (1, N'Pamela', N'Orson', N'e7b0ad0f-cffb-4256-8859-88f80da6ff7c', CAST(N'2023-07-11T21:17:22.7900000' AS DateTime2))
GO
INSERT [dbo].[Employee] ([EmployeeId], [FirstName], [LastName], [rowguid], [ModifiedDate]) VALUES (2, N'David', N'Kandle', N'4e1cf2e9-806b-4870-ad82-4222a0885704', CAST(N'2023-07-11T21:17:22.7900000' AS DateTime2))
GO
INSERT [dbo].[Employee] ([EmployeeId], [FirstName], [LastName], [rowguid], [ModifiedDate]) VALUES (3, N'Jillian', N'Jon', N'711dff0d-5038-4427-9dc9-2bfb760e8ee4', CAST(N'2023-07-11T21:17:22.7900000' AS DateTime2))
GO
SET IDENTITY_INSERT [dbo].[Employee] OFF
GO
INSERT [dbo].[EmployeeAddress] ([EmployeeID], [AddressID], [AddressType], [rowguid], [ModifiedDate]) VALUES (1, 6, N'Home', N'd16b6294-8033-4f72-8c93-dd124b0e2e03', CAST(N'2023-07-11T21:17:22.8266667' AS DateTime2))
GO
INSERT [dbo].[EmployeeAddress] ([EmployeeID], [AddressID], [AddressType], [rowguid], [ModifiedDate]) VALUES (2, 7, N'Home', N'acdf3cbe-ae2e-4483-aa9f-0023c16fe34f', CAST(N'2023-07-11T21:17:22.8266667' AS DateTime2))
GO
INSERT [dbo].[EmployeeAddress] ([EmployeeID], [AddressID], [AddressType], [rowguid], [ModifiedDate]) VALUES (3, 8, N'Home', N'0b8c55ad-0f12-494b-9171-10527cb360ff', CAST(N'2023-07-11T21:17:22.8266667' AS DateTime2))
GO
SET IDENTITY_INSERT [dbo].[Posts] ON 
GO
INSERT [dbo].[Posts] ([PostId], [Title]) VALUES (1, N'Best Boutiques on the Eastside')
GO
INSERT [dbo].[Posts] ([PostId], [Title]) VALUES (2, N'Avoiding over-priced helmets')
GO
INSERT [dbo].[Posts] ([PostId], [Title]) VALUES (3, N'Where to buy Mars Bars')
GO
SET IDENTITY_INSERT [dbo].[Posts] OFF
GO
INSERT [dbo].[PostTag] ([PostId], [TagId]) VALUES (1, 1)
GO
INSERT [dbo].[PostTag] ([PostId], [TagId]) VALUES (2, 1)
GO
INSERT [dbo].[PostTag] ([PostId], [TagId]) VALUES (1, 2)
GO
INSERT [dbo].[PostTag] ([PostId], [TagId]) VALUES (1, 3)
GO
INSERT [dbo].[PostTag] ([PostId], [TagId]) VALUES (3, 3)
GO
INSERT [dbo].[PostTag] ([PostId], [TagId]) VALUES (2, 4)
GO
INSERT [dbo].[PostTag] ([PostId], [TagId]) VALUES (3, 4)
GO
INSERT [dbo].[PricesList] ([PriceListId], [Description], [From], [To]) VALUES (N'de60f9fb-7d4f-489a-9aae-2a7f7e4a5f0a', N'Daly price list', NULL, NULL)
GO
INSERT [dbo].[PricesList] ([PriceListId], [Description], [From], [To]) VALUES (N'944563b4-1f40-4218-b896-7fcb71674f43', N'Hollyday price list', NULL, NULL)
GO
INSERT [dbo].[PricesListCategory] ([PriceListId], [PriceCategoryId]) VALUES (N'de60f9fb-7d4f-489a-9aae-2a7f7e4a5f0a', N'A_BIKES')
GO
INSERT [dbo].[PricesListCategory] ([PriceListId], [PriceCategoryId]) VALUES (N'de60f9fb-7d4f-489a-9aae-2a7f7e4a5f0a', N'A_CLOTHE')
GO
INSERT [dbo].[PricesListCategory] ([PriceListId], [PriceCategoryId]) VALUES (N'de60f9fb-7d4f-489a-9aae-2a7f7e4a5f0a', N'A_COMPT')
GO
INSERT [dbo].[PricesListCategory] ([PriceListId], [PriceCategoryId]) VALUES (N'944563b4-1f40-4218-b896-7fcb71674f43', N'A_BIKES')
GO
INSERT [dbo].[PricesListCategory] ([PriceListId], [PriceCategoryId]) VALUES (N'944563b4-1f40-4218-b896-7fcb71674f43', N'A_CLOTHE')
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'de60f9fb-7d4f-489a-9aae-2a7f7e4a5f0a', N'A_BIKES', N'7e3a1e1d-5a48-4245-b1f2-1e7d2f6266e3', N'9f0c07d0-303c-4860-84ec-921931af4ddc', N'Mountain-200 Black, 46', CAST(2294.99 AS Decimal(18, 2)), CAST(0.00 AS Decimal(18, 2)), 0)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'de60f9fb-7d4f-489a-9aae-2a7f7e4a5f0a', N'A_BIKES', N'f454885e-7be6-40b5-8962-5b5e5d05c151', N'70e6b7a9-9b09-4b8a-9968-e1832d5dc6aa', N'Mountain-300 Black, 38', CAST(1079.99 AS Decimal(18, 2)), CAST(0.00 AS Decimal(18, 2)), 4)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'de60f9fb-7d4f-489a-9aae-2a7f7e4a5f0a', N'A_BIKES', N'a7c6aa37-e23e-428f-8ce6-7a86b2ef90ba', N'9e5d4fcf-f44c-48ff-a381-ad464a4baa1d', N'Mountain-200 Black, 38', CAST(2294.99 AS Decimal(18, 2)), CAST(0.00 AS Decimal(18, 2)), 2)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'de60f9fb-7d4f-489a-9aae-2a7f7e4a5f0a', N'A_BIKES', N'baef21fd-3c04-4b3c-98cc-bb5aa864b46b', N'80796ee7-f3d6-4d16-aef3-d98450bdd276', N'Mountain-200 Black, 42', CAST(2294.99 AS Decimal(18, 2)), CAST(0.00 AS Decimal(18, 2)), 2)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'de60f9fb-7d4f-489a-9aae-2a7f7e4a5f0a', N'A_BIKES', N'e95a807c-1804-41da-ba81-bf7d95827350', N'2040a6d1-3c6a-43d3-8671-7ae442894922', N'Mountain-200 Silver, 38', CAST(2319.99 AS Decimal(18, 2)), CAST(0.00 AS Decimal(18, 2)), 3)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'de60f9fb-7d4f-489a-9aae-2a7f7e4a5f0a', N'A_BIKES', N'10d4f32e-486d-4b39-9210-c158307fab3f', N'3bf9b2b9-8bbc-4933-a7c4-2f78bc200056', N'Mountain-100 Silver, 38', CAST(3399.99 AS Decimal(18, 2)), CAST(0.00 AS Decimal(18, 2)), 1)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'de60f9fb-7d4f-489a-9aae-2a7f7e4a5f0a', N'A_BIKES', N'eb93e5c3-6d64-4722-a9e2-cce10cebf4e6', N'0c47959f-cbbe-4f22-8a6f-c6e52eca74cd', N'Mountain-100 Black, 38', CAST(3374.99 AS Decimal(18, 2)), CAST(0.00 AS Decimal(18, 2)), 4)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'944563b4-1f40-4218-b896-7fcb71674f43', N'A_BIKES', N'0d8ffa8f-32c6-498c-94c0-032573a069f8', N'3bf9b2b9-8bbc-4933-a7c4-2f78bc200056', N'Mountain-100 Silver, 38(Easter 2023)', CAST(3399.99 AS Decimal(18, 2)), CAST(10.00 AS Decimal(18, 2)), 1)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'944563b4-1f40-4218-b896-7fcb71674f43', N'A_BIKES', N'a0c920e5-24b7-4a3b-abb0-26b06feee1c7', N'9f0c07d0-303c-4860-84ec-921931af4ddc', N'Mountain-200 Black, 46(Easter 2023)', CAST(2294.99 AS Decimal(18, 2)), CAST(10.00 AS Decimal(18, 2)), 2)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'944563b4-1f40-4218-b896-7fcb71674f43', N'A_BIKES', N'229533d0-adfa-49e8-919b-2acd0b76e0a8', N'80796ee7-f3d6-4d16-aef3-d98450bdd276', N'Mountain-200 Black, 42(Easter 2023)', CAST(2294.99 AS Decimal(18, 2)), CAST(5.00 AS Decimal(18, 2)), 4)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'944563b4-1f40-4218-b896-7fcb71674f43', N'A_BIKES', N'96472d75-928d-4691-a647-93622d524ef3', N'70e6b7a9-9b09-4b8a-9968-e1832d5dc6aa', N'Mountain-300 Black, 38(Easter 2023)', CAST(1079.99 AS Decimal(18, 2)), CAST(30.00 AS Decimal(18, 2)), 4)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'944563b4-1f40-4218-b896-7fcb71674f43', N'A_BIKES', N'c6ee8fc5-ad09-4389-8282-c0269f48d202', N'9e5d4fcf-f44c-48ff-a381-ad464a4baa1d', N'Mountain-200 Black, 38(Easter 2023)', CAST(2294.99 AS Decimal(18, 2)), CAST(10.00 AS Decimal(18, 2)), 3)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'944563b4-1f40-4218-b896-7fcb71674f43', N'A_BIKES', N'bb263d1c-e015-43fd-a7c2-e7edd57977ef', N'2040a6d1-3c6a-43d3-8671-7ae442894922', N'Mountain-200 Silver, 38(Easter 2023)', CAST(2319.99 AS Decimal(18, 2)), CAST(30.00 AS Decimal(18, 2)), 2)
GO
INSERT [dbo].[PricesListDetail] ([PriceListId], [PriceCategoryId], [PriceListDettailId], [ProductId], [ProductDescription], [Amount], [Discount], [MinQuantity]) VALUES (N'944563b4-1f40-4218-b896-7fcb71674f43', N'A_BIKES', N'5d6c50ee-07cf-499b-a43f-fe6f31b92600', N'0c47959f-cbbe-4f22-8a6f-c6e52eca74cd', N'Mountain-100 Black, 38(Easter 2023)', CAST(3374.99 AS Decimal(18, 2)), CAST(10.00 AS Decimal(18, 2)), 1)
GO
SET IDENTITY_INSERT [dbo].[Tags] ON 
GO
INSERT [dbo].[Tags] ([TagId], [Text]) VALUES (1, N'Golden')
GO
INSERT [dbo].[Tags] ([TagId], [Text]) VALUES (2, N'Pineapple')
GO
INSERT [dbo].[Tags] ([TagId], [Text]) VALUES (3, N'Girlscout')
GO
INSERT [dbo].[Tags] ([TagId], [Text]) VALUES (4, N'Cookies')
GO
SET IDENTITY_INSERT [dbo].[Tags] OFF
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'0997947c-3ed3-44e5-a08a-0950c81da7cf', N'HL Mountain Handlebars', N'HB-M918', NULL, 53.3999, 120.2700, NULL, NULL, N'HANDLB', 55, NULL, NULL, NULL, NULL, NULL, N'1adef45a-5801-4260-b113-5d4ccb845f01', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'e1db3ef8-635f-4cf2-a88b-2ce90d91de25', N'HL Road Frame - Black, 58', N'FR-R92B-58', N'Black', 1059.3100, 1431.5000, N'58', CAST(1016.04 AS Decimal(8, 2)), N'ROADFR', 6, NULL, NULL, NULL, NULL, NULL, N'77681604-0f9b-4f65-9568-03d8a65a3fb7', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'3bf9b2b9-8bbc-4933-a7c4-2f78bc200056', N'Mountain-100 Silver, 38', N'BK-M82S-38', N'Silver', 1912.1544, 3399.9900, N'38', CAST(9230.56 AS Decimal(8, 2)), N'MOUNTB', 19, NULL, NULL, NULL, NULL, NULL, N'6aa49d0c-f084-4de3-a008-0acd8ad542f4', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'd0deb624-b353-4bf0-8134-304f1a038112', N'HL Road Frame - Red, 58', N'FR-R92R-58', N'Red', 1059.3100, 1431.5000, N'58', CAST(1016.04 AS Decimal(8, 2)), N'ROADFR', 6, NULL, NULL, NULL, NULL, NULL, N'b16a38c1-4a48-4086-bf55-a7ab9977683a', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'06454dba-30bf-44e6-b782-6164cd1cf8ec', N'ML Mountain Handlebars', N'HB-M763', NULL, 27.4925, 61.9200, NULL, NULL, N'HANDLB', 54, NULL, NULL, NULL, NULL, NULL, N'4c63488c-5896-4a35-baa2-83392c05e7e9', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'2040a6d1-3c6a-43d3-8671-7ae442894922', N'Mountain-200 Silver, 38', N'BK-M68S-38', N'Silver', 1265.6195, 2319.9900, N'38', CAST(10591.33 AS Decimal(8, 2)), N'MOUNTB', 20, NULL, NULL, NULL, NULL, NULL, N'c1c18dbd-e19a-45ad-9d19-7a86a5307242', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'837d4560-128f-45fc-a16b-84a31819dfe3', N'LL Mountain Handlebars', N'HB-M243', NULL, 19.7758, 44.5400, NULL, NULL, N'HANDLB', 52, NULL, NULL, NULL, NULL, NULL, N'061938f6-a59b-4862-a06c-b0647ff632c2', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'9f0c07d0-303c-4860-84ec-921931af4ddc', N'Mountain-200 Black, 46', N'BK-M68B-46', N'Black', 1251.9813, 2294.9900, N'46', CAST(10945.13 AS Decimal(8, 2)), N'MOUNTB', 20, NULL, NULL, NULL, NULL, NULL, N'33c0be30-38ab-45e5-b996-0576aeb0f5ab', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'7d187938-14cf-476a-9cb4-acac48c76257', N'Road-150 Red, 62', N'BK-R93R-62', N'Red', 2171.2942, 3578.2700, N'62', CAST(6803.85 AS Decimal(8, 2)), N'ROADB', 25, NULL, NULL, NULL, NULL, NULL, N'e855d02d-ed20-442c-baad-7ac107ea7880', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'9e5d4fcf-f44c-48ff-a381-ad464a4baa1d', N'Mountain-200 Black, 38', N'BK-M68B-38', N'Black', 1251.9813, 2294.9900, N'38', CAST(10591.33 AS Decimal(8, 2)), N'MOUNTB', 20, NULL, NULL, NULL, NULL, NULL, N'ce5da49a-2759-4874-988e-36c05183e12a', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'679006ff-df0f-4b28-8283-bdaf038ff976', N'Road-650 Black, 58', N'BK-R50B-58', N'Black', 486.7066, 782.9900, N'58', CAST(8976.55 AS Decimal(8, 2)), N'ROADB', 30, NULL, NULL, NULL, NULL, NULL, N'768dd9e3-82f3-4471-86b0-84dce3844987', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'0c47959f-cbbe-4f22-8a6f-c6e52eca74cd', N'Mountain-100 Black, 38', N'BK-M82B-38', N'Black', 1898.0944, 3374.9900, N'38', CAST(9230.56 AS Decimal(8, 2)), N'MOUNTB', 19, NULL, NULL, NULL, NULL, NULL, N'cedf8360-c416-4cf5-8772-e41890eb7543', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'80796ee7-f3d6-4d16-aef3-d98450bdd276', N'Mountain-200 Black, 42', N'BK-M68B-42', N'Black', 1251.9813, 2294.9900, N'42', CAST(10781.83 AS Decimal(8, 2)), N'MOUNTB', 20, NULL, NULL, NULL, NULL, NULL, N'38763c70-b061-4406-9fc3-6b9264eb88a4', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[Product] ([ProductID], [Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbNailPhoto], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) VALUES (N'70e6b7a9-9b09-4b8a-9968-e1832d5dc6aa', N'Mountain-300 Black, 38', N'BK-M47B-38', N'Black', 598.4354, 1079.9900, N'38', CAST(11498.51 AS Decimal(8, 2)), N'MOUNTB', 21, NULL, NULL, NULL, NULL, NULL, N'b7cefc6d-4e84-4060-9f51-0ea62cd7d7a6', CAST(N'2023-07-11T21:17:22.8666667' AS DateTime2))
GO
INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'A_ACCESS', NULL, N'Accessories', N'36be7f98-7c26-4c6d-b0d2-b18e9fb5b952', CAST(N'2023-07-11T21:17:22.8033333' AS DateTime2), NULL)
GO
INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'A_BIKES', NULL, N'Bikes', N'6a3d7574-d8cb-4201-a886-0790fa118a1c', CAST(N'2023-07-11T21:17:22.8033333' AS DateTime2), NULL)
GO
INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'A_CLOTHE', NULL, N'Clothing', N'14777182-6613-48d3-b957-7845848f6d6e', CAST(N'2023-07-11T21:17:22.8033333' AS DateTime2), NULL)
GO
INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'A_COMPT', NULL, N'Components', N'8bda1ffb-26ad-464c-8545-9713d1ea0691', CAST(N'2023-07-11T21:17:22.8033333' AS DateTime2), NULL)
GO
INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'BRACK', N'A_COMPT', N'Bottom Brackets', N'0f589fde-f095-46de-8e8e-b7e86292d8b7', CAST(N'2023-07-11T21:17:22.8433333' AS DateTime2), NULL)
GO
INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'BRAKES', N'A_COMPT', N'Brakes', N'eaab878b-f96a-48f7-89a1-55c70960e543', CAST(N'2023-07-11T21:17:22.8433333' AS DateTime2), NULL)
GO
INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'HANDLB', N'A_COMPT', N'Handlebars', N'2feaf353-dab9-47ed-9880-19909d9ec392', CAST(N'2023-07-11T21:17:22.8433333' AS DateTime2), NULL)
GO
INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'MOUNTB', N'A_BIKES', N'Mountain Bikes', N'0c07db40-a106-4153-9e5a-c12e05bcc78f', CAST(N'2023-07-11T21:17:22.8433333' AS DateTime2), NULL)
GO
INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'ROADB', N'A_BIKES', N'Road Bikes', N'2c02d8a9-da2f-4a23-8bfc-73e5a73eff31', CAST(N'2023-07-11T21:17:22.8433333' AS DateTime2), NULL)
GO
INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'ROADFR', N'A_COMPT', N'Road Frames', N'c0829da8-2d6a-4472-8b74-45b4c0c5ed06', CAST(N'2023-07-11T21:17:22.8433333' AS DateTime2), NULL)
GO
INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryId], [Name], [rowguid], [ModifiedDate], [Attribute With Space]) VALUES (N'TOURB', N'A_BIKES', N'Touring Bikes', N'46cbab6b-469f-48ad-993a-80b03da46e06', CAST(N'2023-07-11T21:17:22.8433333' AS DateTime2), NULL)
GO
SET IDENTITY_INSERT [SalesLT].[ProductModel] ON 
GO
INSERT [SalesLT].[ProductModel] ([ProductModelID], [Name], [CatalogDescription], [rowguid], [ModifiedDate]) VALUES (6, N'HL Road Frame', NULL, N'97ddf575-0a89-4264-81e2-fc28c32c3a71', CAST(N'2023-07-11T21:17:22.8066667' AS DateTime2))
GO
INSERT [SalesLT].[ProductModel] ([ProductModelID], [Name], [CatalogDescription], [rowguid], [ModifiedDate]) VALUES (19, N'Mountain-100', N'
                        <?xml-stylesheet href="ProductDescription.xsl" type="text/xsl"?><p1:ProductDescription xmlns:p1="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/ProductModelDescription" xmlns:wm="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/ProductModelWarrAndMain" xmlns:wf="http://www.adventure-works.com/schemas/OtherFeatures" xmlns:html="http://www.w3.org/1999/xhtml" ProductModelID="19" ProductModelName="Mountain 100"><p1:Summary><html:p>Our top-of-the-line competition mountain bike. 
                        Performance-enhancing options include the innovative HL Frame,
                        super-smooth front suspension, and traction for all terrain.
                        </html:p></p1:Summary><p1:Manufacturer><p1:Name>AdventureWorks</p1:Name><p1:Copyright>2002</p1:Copyright><p1:ProductURL>HTTP://www.Adventure-works.com</p1:ProductURL></p1:Manufacturer><p1:Features>These are the product highlights. 
                        <wm:Warranty><wm:WarrantyPeriod>3 years</wm:WarrantyPeriod><wm:Description>parts and labor</wm:Description></wm:Warranty><wm:Maintenance><wm:NoOfYears>10 years</wm:NoOfYears><wm:Description>maintenance contract available through your dealer or any AdventureWorks retail store.</wm:Description></wm:Maintenance><wf:wheel>High performance wheels.</wf:wheel><wf:saddle><html:i>Anatomic design</html:i> and made from durable leather for a full-day of riding in comfort.</wf:saddle><wf:pedal><html:b>Top-of-the-line</html:b> clipless pedals with adjustable tension.</wf:pedal><wf:BikeFrame>Each frame is hand-crafted in our Bothell facility to the optimum diameter
                        and wall-thickness required of a premium mountain frame.
                        The heat-treated welded aluminum frame has a larger diameter tube that absorbs the bumps.</wf:BikeFrame><wf:crankset> Triple crankset; alumunim crank arm; flawless shifting. </wf:crankset></p1:Features><!-- add one or more of these elements...one for each specific product in this product model --><p1:Picture><p1:Angle>front</p1:Angle><p1:Size>small</p1:Size><p1:ProductPhotoID>118</p1:ProductPhotoID></p1:Picture><!-- add any tags in <specifications> --><p1:Specifications> These are the product specifications.
                        <Material>Almuminum Alloy</Material><Color>Available in most colors</Color><ProductLine>Mountain bike</ProductLine><Style>Unisex</Style><RiderExperience>Advanced to Professional riders</RiderExperience></p1:Specifications></p1:ProductDescription>
                ', N'172c1551-0552-4fa5-b667-a4d793dd8589', CAST(N'2023-07-11T21:17:22.8066667' AS DateTime2))
GO
INSERT [SalesLT].[ProductModel] ([ProductModelID], [Name], [CatalogDescription], [rowguid], [ModifiedDate]) VALUES (20, N'Mountain-200', NULL, N'e085b9cb-d3c5-4262-8563-abfb17c618ec', CAST(N'2023-07-11T21:17:22.8066667' AS DateTime2))
GO
INSERT [SalesLT].[ProductModel] ([ProductModelID], [Name], [CatalogDescription], [rowguid], [ModifiedDate]) VALUES (21, N'Mountain-300', NULL, N'fdb78776-301e-4e3a-9f46-fe57ec500401', CAST(N'2023-07-11T21:17:22.8066667' AS DateTime2))
GO
INSERT [SalesLT].[ProductModel] ([ProductModelID], [Name], [CatalogDescription], [rowguid], [ModifiedDate]) VALUES (25, N'Road-150', N'
                        <?xml-stylesheet href="ProductDescription.xsl" type="text/xsl"?><p1:ProductDescription xmlns:p1="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/ProductModelDescription" xmlns:wm="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/ProductModelWarrAndMain" xmlns:wf="http://www.adventure-works.com/schemas/OtherFeatures" xmlns:html="http://www.w3.org/1999/xhtml" ProductModelID="25" ProductModelName="Road-150"><p1:Summary><html:p>This bike is ridden by race winners. Developed with the 
                        Adventure Works Cycles professional race team, it has a extremely light
                        heat-treated aluminum frame, and steering that allows precision control.
                        </html:p></p1:Summary><p1:Manufacturer><p1:Name>AdventureWorks</p1:Name><p1:Copyright>2002</p1:Copyright><p1:ProductURL>HTTP://www.Adventure-works.com</p1:ProductURL></p1:Manufacturer><p1:Features>These are the product highlights. 
                        <wm:Warranty><wm:WarrantyPeriod>4 years</wm:WarrantyPeriod><wm:Description>parts and labor</wm:Description></wm:Warranty><wm:Maintenance><wm:NoOfYears>7 years</wm:NoOfYears><wm:Description>maintenance contact available through dealer or any Adventure Works Cycles retail store.</wm:Description></wm:Maintenance><wf:handlebar>Designed for racers; high-end anatomically shaped bar from aluminum alloy.</wf:handlebar><wf:wheel>Strong wheels with double-walled rims.</wf:wheel><wf:saddle><html:i>Lightweight</html:i> kevlar racing saddle.</wf:saddle><wf:pedal><html:b>Top-of-the-line</html:b> clipless pedals with adjustable tension.</wf:pedal><wf:BikeFrame><html:i>Our lightest and best quality</html:i> aluminum frame made from the newest alloy;
                        it is welded and heat-treated for strength.
                        Our innovative design results in maximum comfort and performance.</wf:BikeFrame></p1:Features><!-- add one or more of these elements...one for each specific product in this product model --><p1:Picture><p1:Angle>front</p1:Angle><p1:Size>small</p1:Size><p1:ProductPhotoID>126</p1:ProductPhotoID></p1:Picture><!-- add any tags in <specifications> --><p1:Specifications> These are the product specifications.
                        <Material>Aluminum</Material><Color>Available in all colors.</Color><ProductLine>Road bike</ProductLine><Style>Unisex</Style><RiderExperience>Intermediate to Professional riders</RiderExperience></p1:Specifications></p1:ProductDescription>
                ', N'45818e12-54db-48cf-ab06-2933776064c6', CAST(N'2023-07-11T21:17:22.8066667' AS DateTime2))
GO
INSERT [SalesLT].[ProductModel] ([ProductModelID], [Name], [CatalogDescription], [rowguid], [ModifiedDate]) VALUES (30, N'Road-650', NULL, N'4492418c-6246-4693-b1b2-0f89ffc7c9f5', CAST(N'2023-07-11T21:17:22.8066667' AS DateTime2))
GO
INSERT [SalesLT].[ProductModel] ([ProductModelID], [Name], [CatalogDescription], [rowguid], [ModifiedDate]) VALUES (52, N'LL Mountain Handlebars', NULL, N'ea69c41b-2649-4f66-a211-bdf59e68dcfb', CAST(N'2023-07-11T21:17:22.8066667' AS DateTime2))
GO
INSERT [SalesLT].[ProductModel] ([ProductModelID], [Name], [CatalogDescription], [rowguid], [ModifiedDate]) VALUES (54, N'ML Mountain Handlebars', NULL, N'19ba18cf-2f6e-428e-885a-ce4c85b4eb17', CAST(N'2023-07-11T21:17:22.8066667' AS DateTime2))
GO
INSERT [SalesLT].[ProductModel] ([ProductModelID], [Name], [CatalogDescription], [rowguid], [ModifiedDate]) VALUES (55, N'HL Mountain Handlebars', NULL, N'40700717-9483-40e0-b7ad-289bcff71fbe', CAST(N'2023-07-11T21:17:22.8066667' AS DateTime2))
GO
SET IDENTITY_INSERT [SalesLT].[ProductModel] OFF
GO
SET IDENTITY_INSERT [SalesLT].[SalesOrderDetail] ON 
GO
INSERT [SalesLT].[SalesOrderDetail] ([SalesOrderDetailID], [SalesOrderID], [OrderQty], [ProductID], [UnitPrice], [UnitPriceDiscount], [LineTotal], [rowguid], [ModifiedDate]) VALUES (110562, 1000, 1, N'7d187938-14cf-476a-9cb4-acac48c76257', 3578.2700, 0.0000, NULL, N'a5315912-aee0-47be-a17c-49b60149c317', CAST(N'2023-07-11T21:17:22.8766667' AS DateTime2))
GO
INSERT [SalesLT].[SalesOrderDetail] ([SalesOrderDetailID], [SalesOrderID], [OrderQty], [ProductID], [UnitPrice], [UnitPriceDiscount], [LineTotal], [rowguid], [ModifiedDate]) VALUES (110563, 1000, 2, N'837d4560-128f-45fc-a16b-84a31819dfe3', 44.5400, 0.0000, NULL, N'423bfcb9-a7f2-41d5-8678-3c993b41ddaa', CAST(N'2023-07-11T21:17:22.8766667' AS DateTime2))
GO
INSERT [SalesLT].[SalesOrderDetail] ([SalesOrderDetailID], [SalesOrderID], [OrderQty], [ProductID], [UnitPrice], [UnitPriceDiscount], [LineTotal], [rowguid], [ModifiedDate]) VALUES (110564, 1000, 2, N'd0deb624-b353-4bf0-8134-304f1a038112', 1431.5000, 0.0000, NULL, N'76c43859-3e49-4ae2-85e2-6d3a51338276', CAST(N'2023-07-11T21:17:22.8766667' AS DateTime2))
GO
SET IDENTITY_INSERT [SalesLT].[SalesOrderDetail] OFF
GO
SET IDENTITY_INSERT [SalesLT].[SalesOrderHeader] ON 
GO
INSERT [SalesLT].[SalesOrderHeader] ([SalesOrderID], [RevisionNumber], [OrderDate], [DueDate], [ShipDate], [Status], [OnlineOrderFlag], [SalesOrderNumber], [PurchaseOrderNumber], [AccountNumber], [CustomerID], [ShipToAddressID], [BillToAddressID], [ShipMethod], [CreditCardApprovalCode], [SubTotal], [TaxAmt], [Freight], [TotalDue], [Comment], [rowguid], [ModifiedDate]) VALUES (1000, 1, CAST(N'2008-02-20T13:20:10.0000000' AS DateTime2), CAST(N'2008-02-20T13:20:10.0000000' AS DateTime2), CAST(N'2008-03-05T10:40:30.0000000+02:30' AS DateTimeOffset), 5, 1, N'SO-1000', N'PO348186287', N'10-4020-000609', N'1867e41d-23e9-4496-b84e-e0fd30088db2', 4, 5, N'CAR TRANSPORTATION', NULL, 6530.3500, 70.4279, 22.0087, 6622.7866, NULL, N'f6cf0fe8-c6df-4859-9c3a-fff0d37080b0', CAST(N'2008-10-10T00:00:00.0000000' AS DateTime2))
GO
SET IDENTITY_INSERT [SalesLT].[SalesOrderHeader] OFF
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_Address_City_StateProvince_PostalCode_CountryRegion]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_Address_City_StateProvince_PostalCode_CountryRegion] ON [dbo].[Address]
(
	[City] ASC,
	[StateProvince] ASC,
	[PostalCode] ASC,
	[CountryRegion] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_Address_StateProvince]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_Address_StateProvince] ON [dbo].[Address]
(
	[StateProvince] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_Customer_EmailAddress]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_Customer_EmailAddress] ON [dbo].[Customer]
(
	[EmailAddress] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_Customer_EmployeeID]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_Customer_EmployeeID] ON [dbo].[Customer]
(
	[EmployeeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_CustomerAddress_AddressID]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_CustomerAddress_AddressID] ON [dbo].[CustomerAddress]
(
	[AddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_EmployeeAddress_AddressID]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_EmployeeAddress_AddressID] ON [dbo].[EmployeeAddress]
(
	[AddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_PostTag_TagId]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_PostTag_TagId] ON [dbo].[PostTag]
(
	[TagId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [AK_Product_Name]    Script Date: 11/07/2023 21:19:21 ******/
CREATE UNIQUE NONCLUSTERED INDEX [AK_Product_Name] ON [SalesLT].[Product]
(
	[Name] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [AK_Product_ProductNumber]    Script Date: 11/07/2023 21:19:21 ******/
CREATE UNIQUE NONCLUSTERED INDEX [AK_Product_ProductNumber] ON [SalesLT].[Product]
(
	[ProductNumber] ASC
)
WHERE ([ProductNumber] IS NOT NULL)
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_Product_ProductCategoryID]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_Product_ProductCategoryID] ON [SalesLT].[Product]
(
	[ProductCategoryID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_Product_ProductModelID]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_Product_ProductModelID] ON [SalesLT].[Product]
(
	[ProductModelID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [AK_ProductCategory_Name]    Script Date: 11/07/2023 21:19:21 ******/
CREATE UNIQUE NONCLUSTERED INDEX [AK_ProductCategory_Name] ON [SalesLT].[ProductCategory]
(
	[Name] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_ProductCategory_ParentProductCategoryId]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_ProductCategory_ParentProductCategoryId] ON [SalesLT].[ProductCategory]
(
	[ParentProductCategoryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [AK_ProductModel_Name]    Script Date: 11/07/2023 21:19:21 ******/
CREATE UNIQUE NONCLUSTERED INDEX [AK_ProductModel_Name] ON [SalesLT].[ProductModel]
(
	[Name] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_SalesOrderDetail_ProductID]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_SalesOrderDetail_ProductID] ON [SalesLT].[SalesOrderDetail]
(
	[ProductID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_SalesOrderDetail_SalesOrderID]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_SalesOrderDetail_SalesOrderID] ON [SalesLT].[SalesOrderDetail]
(
	[SalesOrderID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_SalesOrderHeader_BillToAddressID]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_SalesOrderHeader_BillToAddressID] ON [SalesLT].[SalesOrderHeader]
(
	[BillToAddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_SalesOrderHeader_CustomerID]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_SalesOrderHeader_CustomerID] ON [SalesLT].[SalesOrderHeader]
(
	[CustomerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_SalesOrderHeader_ShipToAddressID]    Script Date: 11/07/2023 21:19:21 ******/
CREATE NONCLUSTERED INDEX [IX_SalesOrderHeader_ShipToAddressID] ON [SalesLT].[SalesOrderHeader]
(
	[ShipToAddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Address] ADD  DEFAULT (newid()) FOR [rowguid]
GO
ALTER TABLE [dbo].[Address] ADD  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Customer] ADD  DEFAULT (newid()) FOR [CustomerID]
GO
ALTER TABLE [dbo].[Customer] ADD  DEFAULT (newid()) FOR [rowguid]
GO
ALTER TABLE [dbo].[Customer] ADD  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[CustomerAddress] ADD  DEFAULT (newid()) FOR [rowguid]
GO
ALTER TABLE [dbo].[CustomerAddress] ADD  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Employee] ADD  DEFAULT (newid()) FOR [rowguid]
GO
ALTER TABLE [dbo].[Employee] ADD  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[EmployeeAddress] ADD  DEFAULT (newid()) FOR [rowguid]
GO
ALTER TABLE [dbo].[EmployeeAddress] ADD  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [SalesLT].[Product] ADD  DEFAULT (newid()) FOR [rowguid]
GO
ALTER TABLE [SalesLT].[Product] ADD  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [SalesLT].[ProductCategory] ADD  DEFAULT (newid()) FOR [rowguid]
GO
ALTER TABLE [SalesLT].[ProductCategory] ADD  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [SalesLT].[ProductModel] ADD  DEFAULT (newid()) FOR [rowguid]
GO
ALTER TABLE [SalesLT].[ProductModel] ADD  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [SalesLT].[SalesOrderDetail] ADD  DEFAULT (newid()) FOR [rowguid]
GO
ALTER TABLE [SalesLT].[SalesOrderDetail] ADD  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] ADD  DEFAULT (getdate()) FOR [OrderDate]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] ADD  DEFAULT ((1)) FOR [Status]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] ADD  DEFAULT ((1)) FOR [OnlineOrderFlag]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] ADD  DEFAULT ('SO-XXXX') FOR [SalesOrderNumber]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] ADD  DEFAULT ((0.00)) FOR [SubTotal]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] ADD  DEFAULT ((0.00)) FOR [TaxAmt]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] ADD  DEFAULT ((0.00)) FOR [Freight]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] ADD  DEFAULT ((0.00)) FOR [TotalDue]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] ADD  DEFAULT (newid()) FOR [rowguid]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] ADD  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Customer]  WITH CHECK ADD  CONSTRAINT [FK_Customer_Employee_EmployeeID] FOREIGN KEY([EmployeeID])
REFERENCES [dbo].[Employee] ([EmployeeId])
GO
ALTER TABLE [dbo].[Customer] CHECK CONSTRAINT [FK_Customer_Employee_EmployeeID]
GO
ALTER TABLE [dbo].[CustomerAddress]  WITH CHECK ADD  CONSTRAINT [FK_CustomerAddress_Address_AddressID] FOREIGN KEY([AddressID])
REFERENCES [dbo].[Address] ([AddressID])
GO
ALTER TABLE [dbo].[CustomerAddress] CHECK CONSTRAINT [FK_CustomerAddress_Address_AddressID]
GO
ALTER TABLE [dbo].[CustomerAddress]  WITH CHECK ADD  CONSTRAINT [FK_CustomerAddress_Customer_CustomerID] FOREIGN KEY([CustomerID])
REFERENCES [dbo].[Customer] ([CustomerID])
GO
ALTER TABLE [dbo].[CustomerAddress] CHECK CONSTRAINT [FK_CustomerAddress_Customer_CustomerID]
GO
ALTER TABLE [dbo].[EmployeeAddress]  WITH CHECK ADD  CONSTRAINT [FK_EmployeeAddress_Address_AddressID] FOREIGN KEY([AddressID])
REFERENCES [dbo].[Address] ([AddressID])
GO
ALTER TABLE [dbo].[EmployeeAddress] CHECK CONSTRAINT [FK_EmployeeAddress_Address_AddressID]
GO
ALTER TABLE [dbo].[EmployeeAddress]  WITH CHECK ADD  CONSTRAINT [FK_EmployeeAddress_Employee_EmployeeID] FOREIGN KEY([EmployeeID])
REFERENCES [dbo].[Employee] ([EmployeeId])
GO
ALTER TABLE [dbo].[EmployeeAddress] CHECK CONSTRAINT [FK_EmployeeAddress_Employee_EmployeeID]
GO
ALTER TABLE [dbo].[PostTag]  WITH CHECK ADD  CONSTRAINT [FK_PostTag_Posts] FOREIGN KEY([PostId])
REFERENCES [dbo].[Posts] ([PostId])
GO
ALTER TABLE [dbo].[PostTag] CHECK CONSTRAINT [FK_PostTag_Posts]
GO
ALTER TABLE [dbo].[PostTag]  WITH CHECK ADD  CONSTRAINT [FK_PostTag_Tags] FOREIGN KEY([TagId])
REFERENCES [dbo].[Tags] ([TagId])
GO
ALTER TABLE [dbo].[PostTag] CHECK CONSTRAINT [FK_PostTag_Tags]
GO
ALTER TABLE [dbo].[PricesListCategory]  WITH CHECK ADD  CONSTRAINT [FK_PricesListCategory_PricesList_PriceListId] FOREIGN KEY([PriceListId])
REFERENCES [dbo].[PricesList] ([PriceListId])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[PricesListCategory] CHECK CONSTRAINT [FK_PricesListCategory_PricesList_PriceListId]
GO
ALTER TABLE [dbo].[PricesListDetail]  WITH CHECK ADD  CONSTRAINT [FK_PricesListDetail_PricesListCategory_PriceListId_PriceCategoryId] FOREIGN KEY([PriceListId], [PriceCategoryId])
REFERENCES [dbo].[PricesListCategory] ([PriceListId], [PriceCategoryId])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[PricesListDetail] CHECK CONSTRAINT [FK_PricesListDetail_PricesListCategory_PriceListId_PriceCategoryId]
GO
ALTER TABLE [SalesLT].[Product]  WITH CHECK ADD  CONSTRAINT [FK_Product_ProductCategory_ProductCategoryID] FOREIGN KEY([ProductCategoryID])
REFERENCES [SalesLT].[ProductCategory] ([ProductCategoryID])
GO
ALTER TABLE [SalesLT].[Product] CHECK CONSTRAINT [FK_Product_ProductCategory_ProductCategoryID]
GO
ALTER TABLE [SalesLT].[Product]  WITH CHECK ADD  CONSTRAINT [FK_Product_ProductModel_ProductModelID] FOREIGN KEY([ProductModelID])
REFERENCES [SalesLT].[ProductModel] ([ProductModelID])
GO
ALTER TABLE [SalesLT].[Product] CHECK CONSTRAINT [FK_Product_ProductModel_ProductModelID]
GO
ALTER TABLE [SalesLT].[ProductCategory]  WITH CHECK ADD  CONSTRAINT [FK_ProductCategory_ProductCategory_ParentProductCategoryId] FOREIGN KEY([ParentProductCategoryId])
REFERENCES [SalesLT].[ProductCategory] ([ProductCategoryID])
GO
ALTER TABLE [SalesLT].[ProductCategory] CHECK CONSTRAINT [FK_ProductCategory_ProductCategory_ParentProductCategoryId]
GO
ALTER TABLE [SalesLT].[SalesOrderDetail]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderDetail_Product_ProductID] FOREIGN KEY([ProductID])
REFERENCES [SalesLT].[Product] ([ProductID])
GO
ALTER TABLE [SalesLT].[SalesOrderDetail] CHECK CONSTRAINT [FK_SalesOrderDetail_Product_ProductID]
GO
ALTER TABLE [SalesLT].[SalesOrderDetail]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderDetail_SalesOrderHeader_SalesOrderID] FOREIGN KEY([SalesOrderID])
REFERENCES [SalesLT].[SalesOrderHeader] ([SalesOrderID])
ON DELETE CASCADE
GO
ALTER TABLE [SalesLT].[SalesOrderDetail] CHECK CONSTRAINT [FK_SalesOrderDetail_SalesOrderHeader_SalesOrderID]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_Address_BillTo_AddressID] FOREIGN KEY([BillToAddressID])
REFERENCES [dbo].[Address] ([AddressID])
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_Address_BillTo_AddressID]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_Address_ShipTo_AddressID] FOREIGN KEY([ShipToAddressID])
REFERENCES [dbo].[Address] ([AddressID])
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_Address_ShipTo_AddressID]
GO
ALTER TABLE [SalesLT].[SalesOrderHeader]  WITH CHECK ADD  CONSTRAINT [FK_SalesOrderHeader_Customer_CustomerID] FOREIGN KEY([CustomerID])
REFERENCES [dbo].[Customer] ([CustomerID])
GO
ALTER TABLE [SalesLT].[SalesOrderHeader] CHECK CONSTRAINT [FK_SalesOrderHeader_Customer_CustomerID]
GO
USE [master]
GO
ALTER DATABASE [AdventureWorks] SET  READ_WRITE 
GO
