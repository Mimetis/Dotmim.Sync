
DROP TRIGGER [dbo].[ServiceTickets_delete_trigger];
DROP TRIGGER [dbo].[ServiceTickets_insert_trigger];
DROP TRIGGER [dbo].[ServiceTickets_update_trigger];
DROP TABLE [dbo].[ServiceTickets_tracking];

GO
PRINT N'Dropping [dbo].[scope_info]...';
DROP TABLE [dbo].[scope_info];

GO
DROP PROCEDURE [dbo].[ServiceTickets_bulkdelete];
DROP PROCEDURE [dbo].[ServiceTickets_bulkinsert];
DROP PROCEDURE [dbo].[ServiceTickets_bulkupdate];
DROP PROCEDURE [dbo].[ServiceTickets_delete];
DROP PROCEDURE [dbo].[ServiceTickets_insert];
DROP PROCEDURE [dbo].[ServiceTickets_insertmetadata];
DROP PROCEDURE [dbo].[ServiceTickets_selectchanges];
DROP PROCEDURE [dbo].[ServiceTickets_selectrow];
DROP PROCEDURE [dbo].[ServiceTickets_update];
DROP PROCEDURE [dbo].[ServiceTickets_updatemetadata];
GO
DROP TYPE [dbo].[ServiceTickets_BulkType];



