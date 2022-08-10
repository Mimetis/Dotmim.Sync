import { Box, Collapse, Fab, IconButton, styled, Table, TableBody, TableCell, tableCellClasses, TableHead, TableRow, Link, Chip, Button } from "@mui/material";
import { FiberNew } from "@mui/icons-material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import { SyncLog } from "../models";
import { useState } from "react";
import { convertToDate, convertTypeToString, convertTypeToThemeColor } from "../services";
import { useNavigate } from "react-router-dom";

interface ISyncLogsTableRowComponentProps {
    row?: SyncLog;
}

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

const SyncLogsTableRowComponent: React.FunctionComponent<ISyncLogsTableRowComponentProps> = (props) => {
    const [openHistory, setOpenHistory] = useState(false);
    const navigate = useNavigate();

    return (
        <>
            <StyledTableRow key={props.row.sessionId}>
                <StyledTableCell component="th" scope="row">
                    <IconButton aria-label="expand row" size="small" onClick={() => setOpenHistory(!openHistory)}>
                        {openHistory ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
                    </IconButton>
                </StyledTableCell>
                <StyledTableCell>
                    <Button onClick={() => setOpenHistory(!openHistory)}>{props.row.sessionId} </Button>
                </StyledTableCell>
                <StyledTableCell>
                    <Button onClick={() => {navigate(`/clientDetails/${props.row.clientScopeId}`)}} >
                        {props.row.clientScopeId}
                    </Button>
                </StyledTableCell>
                <StyledTableCell>{props.row.scopeName}</StyledTableCell>
                <StyledTableCell>{convertToDate(props.row.startTime?.toString())}</StyledTableCell>
                <StyledTableCell>
                    <>
                        <Fab variant="extended" size="small" color={convertTypeToThemeColor(props.row.syncType)} aria-label="add">
                            {convertTypeToString(props.row.syncType)}
                        </Fab>
                    </>
                </StyledTableCell>
                <StyledTableCell>
                    <>
                        {props.row.fromTimestamp}
                        {props.row.isNew ? <FiberNew sx={{ verticalAlign: "middle" }} color="primary" /> : ""}
                    </>
                </StyledTableCell>
                <StyledTableCell>{props.row.toTimestamp}</StyledTableCell>
                <StyledTableCell>{props.row.totalChangesApplied ?? 0}</StyledTableCell>
                <StyledTableCell>{props.row.totalChangesSelected ?? 0}</StyledTableCell>
                <StyledTableCell>{props.row.totalResolvedConflicts ?? 0}</StyledTableCell>
            </StyledTableRow>
            <TableRow>
                <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={6}>
                    <Collapse in={openHistory} timeout="auto" unmountOnExit>
                        <Box sx={{ margin: 1 }}>
                            <Table size="small" aria-label="purchases">
                                <TableHead>
                                    <TableRow>
                                        <TableCell>Table</TableCell>
                                        <TableCell>Applied</TableCell>
                                        <TableCell>Selected</TableCell>
                                        <TableCell>Conflicts</TableCell>
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {props.row.details
                                        .filter((sld) => sld.totalChangesApplied > 0 || sld.totalChangesSelected > 0 || sld.totalResolvedConflicts > 0)
                                        .map((historyRow) => (
                                            <TableRow key={historyRow.clientScopeId + historyRow.sessionId + historyRow.tableName}>
                                                <TableCell component="th" scope="row">
                                                    {historyRow.tableName}
                                                </TableCell>
                                                <TableCell>{historyRow.totalChangesApplied ?? 0}</TableCell>
                                                <TableCell>{historyRow.totalChangesSelected ?? 0}</TableCell>
                                                <TableCell>{historyRow.totalResolvedConflicts ?? 0}</TableCell>
                                            </TableRow>
                                        ))}
                                </TableBody>
                            </Table>
                        </Box>
                    </Collapse>
                </TableCell>
            </TableRow>
        </>
    );
};

export default SyncLogsTableRowComponent;
