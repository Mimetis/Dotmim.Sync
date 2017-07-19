/****** Object:  Table [dbo].[ServiceTickets]    Script Date: 03/08/2016 11:03:42 ******/
CREATE TABLE [dbo].[ServiceTickets](
	[ServiceTicketID] [uniqueidentifier] NOT NULL,
	[Title] [nvarchar](max) NOT NULL,
	[Description] [nvarchar](max) NULL,
	[StatusValue] [int] NOT NULL,
	[EscalationLevel] [int] NOT NULL,
	[Opened] [datetime] NULL,
	[Closed] [datetime] NULL,
	[CustomerID] [int] NULL,
 CONSTRAINT [PK_ServiceTickets] PRIMARY KEY CLUSTERED 
(
	[ServiceTicketID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) 

INSERT [dbo].[ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'c3cd62ac-314b-4dbf-b2bb-0d154d59c9b3', N'Titre 3', N'Description 3', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
INSERT [dbo].[ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'04254b03-3dcb-4232-9b74-36f71b7ad26c', N'Titre 4', N'Description 4', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
INSERT [dbo].[ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'3d7c603e-5790-4d99-9737-37a9db95a642', N'Titre Client 1', N'Description Client 1', 1, 0, CAST(N'2016-07-29T17:26:20.720' AS DateTime), NULL, 1)
INSERT [dbo].[ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'aba16ee2-50d0-4d0c-870d-607cf88a74b5', N'Titre 6', N'Description 6', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
INSERT [dbo].[ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'f30bb2f9-6cb3-4957-89b1-65246319e3da', N'Titre 7', N'Description 7', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
INSERT [dbo].[ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'55d080ff-713b-456a-b900-8bd6d310593c', N'Titre 5', N'Description 5', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
INSERT [dbo].[ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'036e6ac5-bda9-4c45-aa0e-ab1f603319c3', N'Titre 2', N'Description 2', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
INSERT [dbo].[ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'2ca388cf-8d9a-4b7a-9bd1-b0bc6617a766', N'Titre 1', N'Description 1', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
INSERT [dbo].[ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'c762b922-737a-448a-a9b6-bcf02e803eb5', N'Titre 8', N'Description 8', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)
INSERT [dbo].[ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID]) VALUES (N'17114fe9-5106-4299-9e1a-bf84be37d992', N'Titre 9', N'Description 9', 1, 0, CAST(N'2016-07-29T16:36:41.733' AS DateTime), NULL, 1)

Select * from sys.tables