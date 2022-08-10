import {
    AppBar,
    Button,
    Chip,
    Grid,
    IconButton,
    Menu,
    MenuItem,
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
import LoupeIcon from "@mui/icons-material/Loupe";
import MoreHorizIcon from "@mui/icons-material/MoreHoriz";
import { useSyncLogs } from "../hooks";
import { useState } from "react";
import { useScopes } from "../hooks/useScopes";
import SetupJsonDialogComponent from "./SetupJsonDialogComponent";
import { Scope } from "../models";

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

const ScopesComponent: React.FunctionComponent = (props) => {
    const scopesQuery = useScopes();

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
            {(!scopesQuery || !scopesQuery.data || scopesQuery.data.length <= 0) && (
                <Typography sx={{ my: 5, mx: 2 }} color="text.secondary" align="center">
                    No scopes saved on server
                </Typography>
            )}
            {scopesQuery && scopesQuery.data && scopesQuery.data.length > 0 && (
                <>
                    <SetupJsonDialogComponent open={setupJsonOpen} setOpen={setSetupJsonOpen} jsonString={setupJsonString} />
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
                                            placeholder="Search scope "
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
                                    <StyledTableCell>a</StyledTableCell>
                                    <StyledTableCell>Name</StyledTableCell>
                                    <StyledTableCell>Setup</StyledTableCell>
                                    <StyledTableCell>Last Sync</StyledTableCell>
                                    <StyledTableCell>Version</StyledTableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {scopesQuery.data.map((row) => (
                                    <StyledTableRow key={row.name}>
                                        <StyledTableCell component="th" scope="row">
                                            <>
                                                <IconButton color="primary" aria-label="upload picture" component="label" onClick={handleClick}>
                                                    <MoreHorizIcon color="primary" />
                                                </IconButton>
                                                <Menu
                                                    id="basic-menu"
                                                    anchorEl={anchorEl}
                                                    open={open}
                                                    onClose={handleClose}
                                                    MenuListProps={{
                                                        "aria-labelledby": "basic-button",
                                                    }}
                                                >
                                                    <MenuItem onClick={handleClose}>Profile</MenuItem>
                                                    <MenuItem onClick={handleClose}>My account</MenuItem>
                                                    <MenuItem onClick={handleClose}>Logout</MenuItem>
                                                </Menu>
                                            </>
                                        </StyledTableCell>
                                        <StyledTableCell>{row.name}</StyledTableCell>
                                        <StyledTableCell>
                                            <Button onClick={() => openJsonDialogClick(row.setup)}>
                                                <Chip sx={{ cursor: "pointer" }} label={JSON.stringify(row.setup).substring(0, 60) + "..."} />
                                            </Button>
                                        </StyledTableCell>
                                        <StyledTableCell>{row.lastsync}</StyledTableCell>
                                        <StyledTableCell>{row.version}</StyledTableCell>
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
