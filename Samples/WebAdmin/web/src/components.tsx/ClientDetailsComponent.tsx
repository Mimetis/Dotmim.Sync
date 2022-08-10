import { AccountCircle, Edit } from "@mui/icons-material";
import { Box, Paper, Typography, TextField, InputLabel, Input, InputAdornment } from "@mui/material";
import * as React from "react";
import { useParams } from "react-router-dom";

const ClientDetailsComponent: React.FunctionComponent = () => {
    const params = useParams();

    return (
        <>
            <Paper sx={{ overflow: "hidden", height: "100%", mb: "1rem" }}>
                <Box
                    component="form"
                    sx={{ "& .MuiInputLabel-root": { mt: "30px", width: "25ch" }, "& .MuiInput-root": { width: "600px" }, ml: 5 }}
                    noValidate
                    autoComplete="off"
                >
                    <Typography variant="button" display="block" gutterBottom>
                        Client Scope
                    </Typography>

                    <InputLabel htmlFor="input-with-icon-adornment">Client Scope id:</InputLabel>
                    <Input defaultValue={params.clientScopeId} readOnly={true} id="input-with-icon-adornment" />

                    <InputLabel htmlFor="input-with-icon-adornment">Client Scope Name:</InputLabel>
                    <Input
                        startAdornment={
                            <InputAdornment position="start">
                                <Edit color="primary" />
                            </InputAdornment>
                        }
                    />

                    <InputLabel htmlFor="input-with-icon-adornment">Last Sync:</InputLabel>
                    <Input defaultValue="2022-03-04 14h23mm32s" id="input-last-sync" readOnly />

                    <InputLabel htmlFor="input-with-icon-adornment">Last Sync Duration:</InputLabel>
                    <Input defaultValue="32s12ms" id="input-last-sync-duration" readOnly />

                    <InputLabel htmlFor="input-with-icon-adornment">Last Sync Timestamp:</InputLabel>
                    <Input defaultValue="3200" id="input-last-sync-timestamp" readOnly />

                </Box>
            </Paper>
        </>
    );
};

export default ClientDetailsComponent;
