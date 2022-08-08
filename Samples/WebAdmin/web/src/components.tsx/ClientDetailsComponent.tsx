import { Paper } from "@mui/material";
import * as React from "react";
import { useParams } from "react-router-dom";

const ClientDetailsComponent: React.FunctionComponent = () => {
    const params = useParams();

    return (
        <>
            <Paper
                sx={{
                    overflow: "hidden",
                    height: "100%",
                    mb: "1rem",
                }}
            >
                <div>{params.clientScopeId}</div>
            </Paper>
        </>
    );
};

export default ClientDetailsComponent;
