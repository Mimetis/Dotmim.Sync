import { AccountCircle, CopyAll, Edit, EventAvailable, People, Person, Save, Schedule, Storage, Sync, UsbRounded } from "@mui/icons-material";
import { Box, Paper, Typography, TextField, InputLabel, Input, InputAdornment, IconButton, Button, Grid } from "@mui/material";
import * as React from "react";
import { useParams } from "react-router-dom";
import SyncLogsComponent from "./SyncLogsComponent";

const ClientDetailsComponent: React.FunctionComponent = () => {
    const params = useParams();

    return (
        <>
            <Box component="div" sx={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gridGap: 30 }}>
                <Paper >
                    <Typography display="block" variant="h6" ml={2}>Client Scope</Typography>

                    <Box sx={{ display: 'flex', padding: '8px', alignItems: 'center' }}>
                        <Storage />
                        <Typography sx={{ fontWeight: 700, marginLeft: '10px', marginRight: '20px' }}>Client Scope id:</Typography>
                        <Typography sx={{ fontWeight: 400 }}>{params.clientScopeId}</Typography>
                        <IconButton><CopyAll /></IconButton>
                    </Box>
                    <Box sx={{ display: 'flex', padding: '8px', alignItems: 'center' }}>
                        <EventAvailable />
                        <Typography sx={{ fontWeight: 700, marginLeft: '10px', marginRight: '20px' }}>Last Sync:</Typography>
                        <Typography sx={{ fontWeight: 400, flex: 1 }}>10/6/2022 1:15:36 PM</Typography>
                    </Box>
                    <Box sx={{ display: 'flex', padding: '8px', alignItems: 'center' }}>
                        <Schedule />
                        <Typography sx={{ fontWeight: 700, marginLeft: '10px', marginRight: '20px' }}>Last sync duration:</Typography>
                        <Typography sx={{ fontWeight: 400, flex: 1 }}>0h 0m 0s 270ms</Typography>
                    </Box>

                </Paper>
                <Paper >

                    <Typography>&nbsp;</Typography>

                    <Box sx={{ display: 'flex', padding: '8px', alignItems: 'center' }}>
                        <Person />
                        <Typography sx={{ fontWeight: 700, marginLeft: '10px', marginRight: '20px' }}>Friendly name:</Typography>
                        <TextField sx={{ fontWeight: 400, flex: 0.5 }} variant="outlined" size="small"></TextField >
                        <Button sx={{ fontWeight: 700, marginLeft: '10px', marginRight: '20px' }} variant="contained" endIcon={<Save />} >Apply</Button>
                    </Box>
                </Paper>
            </Box>

            <Box component="div" sx={{ mt: 4 }}>
                <Paper >
                    <Box component="div" ml={2}>
                        <Typography component={'span'} variant="h6"  >Actions</Typography>
                        <Typography component={'span'} variant="caption" ml={1} >(on next sync)</Typography>
                    </Box>

                    <Grid container rowSpacing={1} ml='10px' alignItems={"center"}>
                        <Grid item xs={2}>
                            <Typography sx={{ fontWeight: 700 }}>Normal sync:</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Button sx={{ fontWeight: 700 }} variant="text" endIcon={<Sync />} >Apply on next sync</Button>
                        </Grid>
                        <Grid item xs={2}>
                            <Typography sx={{ fontWeight: 700 }}>Reinitialize with upload:</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Button sx={{ fontWeight: 700 }} variant="text" endIcon={<Sync />} >Apply on next sync</Button>
                        </Grid>
                        <Grid item xs={2}>
                            <Typography sx={{ fontWeight: 700 }}>Reinitialize:</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Button sx={{ fontWeight: 700 }} variant="text" endIcon={<Sync />} >Apply on next sync</Button>
                        </Grid>
                        <Grid item xs={2}>
                            <Typography sx={{ fontWeight: 700 }}>Drop all and sync:</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Button sx={{ fontWeight: 700 }} variant="text" endIcon={<Sync />} >Apply on next sync</Button>
                        </Grid>
                        <Grid item xs={2}>
                            <Typography sx={{ fontWeight: 700 }}>Drop all and exit:</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Button sx={{ fontWeight: 700 }} variant="text" endIcon={<Sync />} >Apply on next sync</Button>
                        </Grid>
                        <Grid item xs={2}>
                            <Typography sx={{ fontWeight: 700 }}>Deprovsion:</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Button sx={{ fontWeight: 700 }} variant="text" endIcon={<Sync />} >Apply on next sync</Button>
                        </Grid>
                        <Grid item xs={2}>
                            <Typography sx={{ fontWeight: 700 }}>Abort sync:</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Button sx={{ fontWeight: 700 }} variant="text" endIcon={<Sync />} >Apply on next sync</Button>
                        </Grid>
                    </Grid>
                </Paper>
            </Box>

            <Box component="div" sx={{ mt: 4 }} >
                <Paper sx={{ backgroundColor: 'success.main', color: 'common.white' }} >
                    <Grid container rowSpacing={1} ml='10px' alignItems={"center"}>
                        <Grid item xs={2}>
                            <Typography sx={{ fontWeight: 700 }}  >Current action on next sync:</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Typography sx={{ fontWeight: 700 }}  >Normal sync</Typography>
                        </Grid>
                    </Grid>
                </Paper>
            </Box>

            <Box component='div' sx={{ mt: 4 }}>
                <SyncLogsComponent clientScopeId={params.clientScopeId} />
            </Box>
        </>
    );
};

export default ClientDetailsComponent;
