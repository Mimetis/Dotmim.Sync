import { Box, Button } from "@mui/material";
import ClientDetailsComponent from "../components.tsx/ClientDetailsComponent";
import ClientsComponent from "../components.tsx/ClientsComponent";

const ScopesPage: React.FunctionComponent = () => {
    return (
        <>
            <ClientDetailsComponent />
            <Box sx={{ flex: 1, display: "flex", flexDirection: "row", alignItems: "flex-end" }}>
                <Button aria-label="Apply" variant="outlined" sx={{ width: "100px" }}>
                    Apply
                </Button>
                <Button variant="contained">Contained</Button>
                <Button variant="contained" disabled>
                    Disabled
                </Button>
                <Button variant="contained" href="#contained-buttons">
                    Link
                </Button>
            </Box>
        </>
    );
};

export default ScopesPage;
