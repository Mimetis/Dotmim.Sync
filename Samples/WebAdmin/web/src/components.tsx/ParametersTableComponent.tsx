import { TableContainer, Paper, Table, TableHead, TableRow, TableBody, TableCell, styled, tableCellClasses } from '@mui/material';
import * as React from 'react';
import SyncLogsTableRowComponent from './SyncLogsTableRowComponent';

interface IParametersTableComponentProps {

  parametersString: string;

}

const ParametersTableComponent: React.FunctionComponent<IParametersTableComponentProps> = (props) => {

  const parameters = JSON.parse(props.parametersString);

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
            <StyledTableCell>Name</StyledTableCell>
            <StyledTableCell>Value</StyledTableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {parameters.map((row: any) => (
            <StyledTableRow key={row.pn}>
              <StyledTableCell>{row.pn}</StyledTableCell>
              <StyledTableCell>{row.v}</StyledTableCell>
            </StyledTableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>

  );
};

export default ParametersTableComponent;
