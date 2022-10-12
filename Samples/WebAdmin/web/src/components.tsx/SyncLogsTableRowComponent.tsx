import { Box, Fab, IconButton, styled, TableCell, tableCellClasses, TableRow, Button, Dialog, DialogContent, DialogTitle, Tooltip, tableRowClasses, TableCellProps, TableRowProps } from "@mui/material";
import { Add, ZoomIn, AltRoute, ErrorOutline, Remove, Check, PriorityHighRounded, Dns, Wifi } from "@mui/icons-material";
import { SyncLog } from "../models";
import { useState } from "react";
import { convertStateToIcon, convertToDate, convertToDurationString, convertTypeToString, convertTypeToThemeColor, getDuration } from "../services";
import { useNavigate } from "react-router-dom";
import ParametersTableComponent from "./ParametersTableComponent";
import SelectedChangesTableComponent from "./SelectedChangesComponent";
import AppliedChangesTableComponent from "./AppliedChangesComponent";

interface ISyncLogsTableRowComponentProps {
    row?: SyncLog;
}


const StyledTableCell = styled(TableCell)<TableCellProps>(({ theme }) => ({
    [`&.${tableCellClasses.head}`]: {
        backgroundColor: theme.palette.primary.main,
        color: theme.palette.common.white,
    },
    [`&.${tableCellClasses.body}`]: {
        fontSize: 12,
    },
}));

const StyledTableRow = styled(TableRow)<TableRowProps & { duration?: number, errorhappened?:  string | undefined }>(({ theme, duration, errorhappened }) => ({
    "&:nth-of-type(odd)": {
        backgroundColor: theme.palette.action.hover,
    },
    // hide last border
    "&:last-child td, &:last-child th": {
        border: 0,
    },
    ...(duration && duration > 10000 && {
        [`&.${tableRowClasses.root}`]: {
            border: '2px solid #e9cd93',
            backgroundColor: '#a5810c1a',
        },
    }),
    ...(errorhappened && {
        [`&.${tableRowClasses.root}`]: {
            border: '2px solid red',
            backgroundColor: '#ff03031a',
        },
    })
}));

const SyncLogsTableRowComponent: React.FunctionComponent<ISyncLogsTableRowComponentProps> = (props) => {
    const navigate = useNavigate();

    const [paramsOpen, setParamsOpen] = useState<boolean>(false);
    const [clientChangesSelected, setClientChangesSelected] = useState(false);
    const [serverChangesSelected, setServerChangesSelected] = useState(false);
    const [changesAppliedOnClient, setChangesAppliedOnClient] = useState(false);
    const [changesAppliedOnServer, setChangesAppliedOnServer] = useState(false);

    let clientChangesSum: { n: string, s: string, u?: number, d?: number };
    let serverChangesSum: { n: string, s: string, u?: number, d?: number };
    let serverAppliedSum: { tn: string, sn: string, st?: number; a?: number, rc?: number, f?: number };
    let clientAppliedSum: { tn: string, sn: string, st?: number; a?: number, rc?: number, f?: number };

    if (props.row.clientChangesSelected) {
        const clientChanges = JSON.parse(props.row.clientChangesSelected);

        if (clientChanges && clientChanges.tcs && clientChanges.tcs.length > 0) {
            const tcs = clientChanges.tcs as Array<{ n: string, s: string, u?: number, d?: number }>;
            clientChangesSum = tcs.reduce((acc, cur) => {

                acc.u += cur.u ? cur.u : 0;
                acc.d += cur.d ? cur.d : 0;
                return acc;
            });
        }
    }

    if (props.row.serverChangesSelected) {
        const serverChanges = JSON.parse(props.row.serverChangesSelected);

        if (serverChanges && serverChanges.tcs && serverChanges.tcs.length > 0) {
            const tcs = serverChanges.tcs as Array<{ n: string, s: string, u?: number, d?: number }>;
            serverChangesSum = tcs.reduce((acc, cur) => {
                acc.u += cur.u ? cur.u : 0;
                acc.d += cur.d ? cur.d : 0;
                return acc;
            });
        }
    }

    if (props.row.changesAppliedOnServer) {
        const changesApplied = JSON.parse(props.row.changesAppliedOnServer);

        if (changesApplied && changesApplied.tca && changesApplied.tca.length > 0) {
            const tca = changesApplied.tca as Array<{ tn: string, sn: string, st?: number; a?: number, rc?: number, f?: number }>;

            serverAppliedSum = tca.reduce((acc, cur) => {
                acc.a += cur.a ? cur.a : 0;
                acc.rc += cur.rc ? cur.rc : 0;
                acc.f += cur.f ? cur.f : 0;
                return acc;
            });
        }
    }

    if (props.row.changesAppliedOnClient) {
        const changesApplied = JSON.parse(props.row.changesAppliedOnClient);

        if (changesApplied && changesApplied.tca && changesApplied.tca.length > 0) {
            const tca = changesApplied.tca as Array<{ tn: string, sn: string, st?: number; a?: number, rc?: number, f?: number }>;

            clientAppliedSum = tca.reduce((acc, cur) => {
                acc.a += cur.a ? cur.a : 0;
                acc.rc += cur.rc ? cur.rc : 0;
                acc.f += cur.f ? cur.f : 0;
                return acc;
            });
        }
    }


    return (
        <>
            <StyledTableRow key={props.row.sessionId}
                duration={getDuration(props.row.startTime?.toString(), props.row.endTime?.toString())} errorhappened={props.row.error ? 'true' : undefined}>
                <StyledTableCell>
                    <Button onClick={() => { navigate(`/clientDetails/${props.row.clientScopeId}`) }} >
                        {props.row.clientScopeId}
                    </Button>
                </StyledTableCell>
                <StyledTableCell sx={{ fontWeight: 'bold' }}>{props.row.scopeName}</StyledTableCell>
                <StyledTableCell>
                    {props.row.scopeParameters &&
                        <>
                            <Dialog maxWidth="lg" open={paramsOpen} onClose={() => setParamsOpen(false)} scroll="paper" >
                                <DialogTitle>Parameters</DialogTitle>
                                <DialogContent dividers>
                                    <ParametersTableComponent parametersString={props.row.scopeParameters} />
                                </DialogContent>
                            </Dialog>
                            <IconButton onClick={() => setParamsOpen(true)}>
                                <ZoomIn color="info" />
                            </IconButton>
                        </>
                    }
                </StyledTableCell>
                <StyledTableCell>{convertStateToIcon({ state: props.row.state })}</StyledTableCell>
                <StyledTableCell>{convertToDate(props.row.startTime?.toString())}</StyledTableCell>
                <StyledTableCell sx={{ fontWeight: 'bold' }}>{convertToDurationString(props.row.startTime?.toString(), props.row.endTime?.toString())}</StyledTableCell>
                <StyledTableCell>
                    <Fab variant="extended" size="small" color={convertTypeToThemeColor(props.row.syncType)} aria-label="add">
                        {convertTypeToString(props.row.syncType)}
                    </Fab>
                </StyledTableCell>
                <StyledTableCell sx={{ fontWeight: 'bold' }} align="right">
                    {props.row.clientChangesSelected &&
                        <>
                            {clientChangesSum && clientChangesSum.u > 0 && <><Box component='span' sx={{ verticalAlign: 'text-top' }}>{clientChangesSum.u}</Box><Add sx={{ verticalAlign: 'middle' }} color="primary" fontSize="small" />&nbsp;</>}
                            {clientChangesSum && clientChangesSum.d > 0 && <><Box component='span' sx={{ verticalAlign: 'text-top' }}>{clientChangesSum.d}</Box><Remove sx={{ verticalAlign: 'middle' }} color="primary" fontSize="small" />&nbsp;</>}
                            <Dialog maxWidth="lg" open={clientChangesSelected} onClose={() => setClientChangesSelected(false)} scroll="paper" >
                                <DialogTitle>Client changes selected</DialogTitle>
                                <DialogContent dividers>
                                    <SelectedChangesTableComponent SelectedChangesString={props.row.clientChangesSelected} />
                                </DialogContent>
                            </Dialog>

                            <IconButton color="primary" onClick={() => setClientChangesSelected(true)}>
                                <ZoomIn />
                            </IconButton>
                        </>
                    }
                </StyledTableCell>
                <StyledTableCell sx={{ fontWeight: 'bold' }} align="right">
                    {props.row.changesAppliedOnServer &&
                        <>
                            {serverAppliedSum && serverAppliedSum.a > 0 && <><Box component='span' sx={{ color: 'success.main', verticalAlign: 'text-top' }}>{serverAppliedSum.a}</Box><Check sx={{ verticalAlign: 'middle' }} color="success" fontSize="small" />&nbsp;</>}
                            {serverAppliedSum && serverAppliedSum.rc > 0 && <><Box component='span' sx={{ color: 'secondary.main', verticalAlign: 'text-top' }}>{serverAppliedSum.rc}</Box><AltRoute sx={{ verticalAlign: 'middle' }} color="secondary" fontSize="small" />&nbsp;</>}
                            {serverAppliedSum && serverAppliedSum.f > 0 && <><Box component='span' sx={{ color: 'error.main', verticalAlign: 'text-top' }}>{serverAppliedSum.f}</Box><ErrorOutline sx={{ verticalAlign: 'middle' }} color="error" fontSize="small" /></>}

                            <Dialog maxWidth="lg" open={changesAppliedOnServer} onClose={() => setChangesAppliedOnServer(false)} scroll="paper" >
                                <DialogTitle>Changes applied on server</DialogTitle>
                                <DialogContent dividers>
                                    <AppliedChangesTableComponent AppliedChangesString={props.row.changesAppliedOnServer} />
                                </DialogContent>
                            </Dialog>

                            <IconButton color="primary" onClick={() => setChangesAppliedOnServer(true)}>
                                <ZoomIn />
                            </IconButton>
                        </>
                    }
                </StyledTableCell>
                <StyledTableCell sx={{ fontWeight: 'bold' }} align="right" >
                    {props.row.serverChangesSelected &&

                        <div >
                            {serverChangesSum && serverChangesSum.u > 0 && <><Box component='span' sx={{ verticalAlign: 'text-top' }}>{serverChangesSum.u}</Box><Add sx={{ verticalAlign: 'middle' }} color="primary" fontSize="small" />&nbsp;</>}
                            {serverChangesSum && serverChangesSum.d > 0 && <><Box component='span' sx={{ verticalAlign: 'text-top' }}>{serverChangesSum.d}</Box><Remove sx={{ verticalAlign: 'middle' }} color="primary" fontSize="small" />&nbsp;</>}

                            <Dialog maxWidth="lg" open={serverChangesSelected} onClose={() => setServerChangesSelected(false)} scroll="paper" >
                                <DialogTitle>Server changes selected</DialogTitle>
                                <DialogContent dividers>
                                    <SelectedChangesTableComponent SelectedChangesString={props.row.serverChangesSelected} />
                                </DialogContent>
                            </Dialog>
                            <IconButton color="primary" onClick={() => setServerChangesSelected(true)}>
                                <ZoomIn />
                            </IconButton>
                        </div>
                    }
                </StyledTableCell>
                <StyledTableCell sx={{ fontWeight: 'bold' }} align="right">
                    {props.row.changesAppliedOnClient &&
                        <>
                            {clientAppliedSum && clientAppliedSum.a > 0 && <><Box component='span' sx={{ color: 'success.main', verticalAlign: 'text-top' }}>{clientAppliedSum.a}</Box><Check sx={{ verticalAlign: 'middle' }} color="success" fontSize="small" />&nbsp;</>}
                            {clientAppliedSum && clientAppliedSum.rc > 0 && <><Box component='span' sx={{ color: 'secondary.main', verticalAlign: 'text-top' }}>{clientAppliedSum.rc}</Box><AltRoute sx={{ verticalAlign: 'middle' }} color="secondary" fontSize="small" />&nbsp;</>}
                            {clientAppliedSum && clientAppliedSum.f > 0 && <><Box component='span' sx={{ color: 'error.main', verticalAlign: 'text-top' }}>{clientAppliedSum.f}</Box><PriorityHighRounded sx={{ verticalAlign: 'middle' }} color="error" fontSize="small" /></>}

                            <Dialog maxWidth="lg" open={changesAppliedOnClient} onClose={() => setChangesAppliedOnClient(false)} scroll="paper" >
                                <DialogTitle>Changes applied on client</DialogTitle>
                                <DialogContent dividers>
                                    <AppliedChangesTableComponent AppliedChangesString={props.row.changesAppliedOnClient} />
                                </DialogContent>
                            </Dialog>

                            <IconButton color="primary" onClick={() => setChangesAppliedOnClient(true)}>
                                <ZoomIn />
                            </IconButton>
                        </>
                    }
                </StyledTableCell>
                <StyledTableCell>
                    {props.row.network && props.row.network == 'Tcp' && <Tooltip title='TCP sync.' ><Box component='span' sx={{ color: 'success.main', verticalAlign: 'baseline' }}><Dns /></Box></Tooltip>}
                    {props.row.network && props.row.network == 'Http' && <Wifi />}
                </StyledTableCell>
            </StyledTableRow>
        </>
    );
};

export default SyncLogsTableRowComponent;
