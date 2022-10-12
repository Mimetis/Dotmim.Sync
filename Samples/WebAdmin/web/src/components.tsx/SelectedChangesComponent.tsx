import { TableContainer, Paper, Table, TableHead, TableRow, TableBody, TableCell, styled, tableCellClasses } from '@mui/material';
import * as React from 'react';

interface ISelectedChangesTableComponentProps {

  SelectedChangesString: string;

}

const SelectedChangesTableComponent: React.FunctionComponent<ISelectedChangesTableComponentProps> = (props) => {

  const SelectedChanges = JSON.parse(props.SelectedChangesString);

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
            <StyledTableCell>Insert/Updates</StyledTableCell>
            <StyledTableCell>Deletes</StyledTableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {SelectedChanges.tcs.map((row: { n: string, sn: string, u?: number, d?: number }) => (
            <StyledTableRow key={row.n + row.sn}>
              <StyledTableCell>{row.sn ? `${row.sn}.${row.n}` : row.n}</StyledTableCell>
              <StyledTableCell align='right'>{row.u ? row.u : 0}</StyledTableCell>
              <StyledTableCell align='right'>{row.d ? row.d : 0}</StyledTableCell>
            </StyledTableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>

  );
};

export default SelectedChangesTableComponent;
