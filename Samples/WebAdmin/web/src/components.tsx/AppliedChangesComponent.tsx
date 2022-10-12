import { TableContainer, Paper, Table, TableHead, TableRow, TableBody, TableCell, styled, tableCellClasses } from '@mui/material';
import * as React from 'react';
import { convertSyncRowStateToString } from '../services';

interface IAppliedChangesTableComponentProps {

  AppliedChangesString: string;

}

const AppliedChangesTableComponent: React.FunctionComponent<IAppliedChangesTableComponentProps> = (props) => {

  const AppliedChanges = JSON.parse(props.AppliedChangesString);

  const StyledTableCell = styled(TableCell)(({ theme }) => ({
    [`&.${tableCellClasses.head}`]: {
      backgroundColor: theme.palette.primary.main,
      color: theme.palette.common.white,
    },
    [`&.${tableCellClasses.body}`]: {
      fontSize: 12,

    },
  }));

  const StyledTableRow = styled(TableRow)(({ theme }) => ({
    "&:nth-of-type(odd)": {
      backgroundColor: theme.palette.action.hover,
    },
    // hide last border
    "&:last-child td, &:last-child th": {
      border: 0,
    },
  }));
  return (
    <TableContainer component={Paper}>
      <Table size="medium" aria-label="simple table">
        <TableHead>
          <TableRow>
            <StyledTableCell>Table</StyledTableCell>
            <StyledTableCell>State</StyledTableCell>
            <StyledTableCell>Applied</StyledTableCell>
            <StyledTableCell>Resolved conflicts</StyledTableCell>
            <StyledTableCell>Failed</StyledTableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {AppliedChanges.tca.map((row: any) => (
            <StyledTableRow key={row.tn + row.sn}>
              <StyledTableCell sx={{ fontWeight: 'bold' }} >{row.sn ? `${row.sn}.${row.tn}` : row.tn}</StyledTableCell>
              <StyledTableCell >{convertSyncRowStateToString(row.st)}</StyledTableCell>
              <StyledTableCell align='right' sx={row.a ? { color: 'green', fontWeight: 'bold' } : {}}>{row.a ? row.a : 0}</StyledTableCell>
              <StyledTableCell align='right' sx={row.rc ? { color: 'blue', fontWeight: 'bold' } : {}}>{row.rc ? row.rc : 0}</StyledTableCell>
              <StyledTableCell align='right' sx={row.f ? { color: 'red', fontWeight: 'bold' } : {}}>{row.f ? row.f : 0}</StyledTableCell>
            </StyledTableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>

  );
};

export default AppliedChangesTableComponent;
