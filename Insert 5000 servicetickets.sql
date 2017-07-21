Delete from ServiceTickets where  SUBSTRING(title, 1, 1) ='_'

declare @cpt int = 0;

while (@cpt < 5000)
Begin
	INSERT [ServiceTickets] ([ServiceTicketID], [Title], [Description], [StatusValue], [EscalationLevel], [Opened], [Closed], [CustomerID])
	VALUES (newid(),  '_Titre ' + cast(@cpt as varchar(5)),  N'Description 9', 1, 0, getdate(), NULL, 1);
	set @cpt = @cpt +1;
End

