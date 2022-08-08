import { AppBar, Grid, IconButton, Paper, styled, Table, TableBody, TableCell, tableCellClasses, TableContainer, TableHead, TableRow, TextField, Toolbar, Tooltip, Typography } from "@mui/material";
import SearchIcon from '@mui/icons-material/Search';
import RefreshIcon from '@mui/icons-material/Refresh';
import { useSyncLogs } from "../hooks";
import { useState } from "react";
import SyncLogsTableRowComponent from "./SyncLogsTableRowComponent";

const StyledTableCell = styled(TableCell)(({ theme }) => ({
  [`&.${tableCellClasses.head}`]: {
    backgroundColor: theme.palette.primary.main,
    color: theme.palette.common.white,
  },
  [`&.${tableCellClasses.body}`]: {
    fontSize: 12,

  },
}));

const SyncLogsComponent: React.FunctionComponent = () => {
  const syncLogsQuery = useSyncLogs();

  return (
    <>
      {(!syncLogsQuery || !syncLogsQuery.data || syncLogsQuery.data.length <= 0) && (
        <Typography sx={{ my: 5, mx: 2 }} color="text.secondary" align="center">
          No logs
        </Typography>
      )}
      {syncLogsQuery && syncLogsQuery.data && syncLogsQuery.data.length > 0 && (

        <>
          <Paper sx={{
            overflow: 'hidden',
            borderBottomLeftRadius: '0px',
            borderBottomRightRadius: '0px',
            borderBottom: 'none',
          }}>

            <AppBar
              position="static"
              color="default"
              elevation={0}
            >
              <Toolbar>
                <Grid container spacing={2} alignItems="center">
                  <Grid item>
                    <SearchIcon color="inherit" sx={{ display: 'block' }} />
                  </Grid>
                  <Grid item xs>
                    <TextField
                      fullWidth
                      placeholder="Search sync session "
                      InputProps={{
                        disableUnderline: true,
                        sx: { fontSize: 'default' },
                      }}
                      variant="standard"
                    />
                  </Grid>
                  <Grid item>
                    <Tooltip title="Reload">
                      <IconButton>
                        <RefreshIcon color="inherit" sx={{ display: 'block' }} />
                      </IconButton>
                    </Tooltip>
                  </Grid>
                </Grid>
              </Toolbar>
            </AppBar>
          </Paper>
          <TableContainer component={Paper} sx={{
            borderTopLeftRadius: '0px',
            borderTopRightRadius: '0px',
            borderTop: 'none'
          }}>
            <Table size="small" aria-label="simple table">
              <TableHead>
                <TableRow>
                  <StyledTableCell></StyledTableCell>
                  <StyledTableCell sortDirection='desc' >Session Id</StyledTableCell>
                  <StyledTableCell>Client Scope Id</StyledTableCell>
                  <StyledTableCell>Scope Name</StyledTableCell>
                  <StyledTableCell>Start Time</StyledTableCell>
                  <StyledTableCell>Type</StyledTableCell>
                  <StyledTableCell>From Timestamp</StyledTableCell>
                  <StyledTableCell>To Timestamp</StyledTableCell>
                  <StyledTableCell>Applied</StyledTableCell>
                  <StyledTableCell>Selected</StyledTableCell>
                  <StyledTableCell>Conflicts</StyledTableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {syncLogsQuery.data.map((row) => (
                  <SyncLogsTableRowComponent key={row.sessionId + row.clientScopeId} row={row} />
                ))}
            </TableBody>
          </Table>
        </TableContainer>
        </>
  )
}

    </>
  );
}

export default SyncLogsComponent;