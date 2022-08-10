import {
    AppBar,
    Button,
    Chip,
    Grid,
    IconButton,
    Link,
    Paper,
    styled,
    Table,
    TableBody,
    TableCell,
    tableCellClasses,
    TableContainer,
    TableHead,
    TableRow,
    TextField,
    Toolbar,
    Tooltip,
    Typography,
} from "@mui/material";
import SearchIcon from "@mui/icons-material/Search";
import RefreshIcon from "@mui/icons-material/Refresh";
import { useClientScopes } from "../hooks";
import { useState } from "react";
import { convertToDate, convertToTime } from "../services";

const StyledTableCell = styled(TableCell)(({ theme }) => ({
    [`&.${tableCellClasses.head}`]: {
        backgroundColor: theme.palette.primary.main,
        color: theme.palette.common.white,
    },
    [`&.${tableCellClasses.body}`]: {
        fontSize: 14,
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

const TableButton = styled(Button)({
    borderRadius: "1px",
});

const ScopesComponent: React.FunctionComponent = (props) => {
    const clientsQuery = useClientScopes();

    const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
    const open = Boolean(anchorEl);

    const [setupJsonString, setSetupJsonString] = useState<string>();
    const [setupJsonOpen, setSetupJsonOpen] = useState<boolean>(false);

    const handleClick = (event: React.MouseEvent<HTMLLabelElement>) => {
        setAnchorEl(event.currentTarget);
    };
    const handleClose = () => {
        setAnchorEl(null);
    };

    const openJsonDialogClick = (jsonString: string) => {
        setSetupJsonString(jsonString);
        setSetupJsonOpen(true);
        console.log("openJsonDialogClick");
    };

    return (
        <>
            {(!clientsQuery || !clientsQuery.data || clientsQuery.data.length <= 0) && (
                <Typography sx={{ my: 5, mx: 2 }} color="text.secondary" align="center">
                    No scopes saved on server
                </Typography>
            )}
            {clientsQuery && clientsQuery.data && clientsQuery.data.length > 0 && (
                <>
                    <Paper
                        sx={{
                            overflow: "hidden",
                            borderBottomLeftRadius: "0px",
                            borderBottomRightRadius: "0px",
                            borderBottom: "none",
                        }}
                    >
                        <AppBar position="static" color="default" elevation={0}>
                            <Toolbar>
                                <Grid container spacing={2} alignItems="center">
                                    <Grid item>
                                        <SearchIcon color="inherit" sx={{ display: "block" }} />
                                    </Grid>
                                    <Grid item xs>
                                        <TextField
                                            placeholder="Search client"
                                            InputProps={{
                                                disableUnderline: true,
                                                sx: { fontSize: "default" },
                                            }}
                                            variant="standard"
                                        />
                                    </Grid>
                                    <Grid item>
                                        <Tooltip title="Reload">
                                            <IconButton>
                                                <RefreshIcon color="inherit" sx={{ display: "block" }} />
                                            </IconButton>
                                        </Tooltip>
                                    </Grid>
                                </Grid>
                            </Toolbar>
                        </AppBar>
                    </Paper>
                    <TableContainer
                        component={Paper}
                        sx={{
                            borderTopLeftRadius: "0px",
                            borderTopRightRadius: "0px",
                            borderTop: "none",
                        }}
                    >
                        <Table size="small" aria-label="simple table">
                            <TableHead>
                                <TableRow>
                                    <StyledTableCell>Client Scope Id</StyledTableCell>
                                    <StyledTableCell>Scope Name</StyledTableCell>
                                    <StyledTableCell>Last Sync</StyledTableCell>
                                    <StyledTableCell>Last Sync Duration</StyledTableCell>
                                    <StyledTableCell>Last Sync Timestamp</StyledTableCell>
                                    <StyledTableCell>Properties</StyledTableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {clientsQuery.data.map((row) => (
                                    <StyledTableRow key={row.id}>
                                        <StyledTableCell>
                                            <Link href={`/clientDetails/${row.id}`} underline="none">
                                                {row.id}
                                            </Link>
                                        </StyledTableCell>
                                        <StyledTableCell>{row.scopeName}</StyledTableCell>
                                        <StyledTableCell>{convertToDate(row.lastSync?.toString())}</StyledTableCell>
                                        <StyledTableCell>{convertToTime(row.lastSyncDuration / 1000)}</StyledTableCell>
                                        <StyledTableCell>{row.lastSyncTimestamp}</StyledTableCell>
                                        <StyledTableCell>
                                            <Button onClick={() => openJsonDialogClick(row.properties)}>
                                                <Chip sx={{ cursor: "pointer" }} label={JSON.stringify(row.properties).substring(0, 60) + "..."} />
                                            </Button>
                                        </StyledTableCell>
                                    </StyledTableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                </>
            )}
        </>
    );
};

export default ScopesComponent;
