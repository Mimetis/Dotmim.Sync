import Divider from "@mui/material/Divider";
import Drawer, { DrawerProps } from "@mui/material/Drawer";
import List from "@mui/material/List";
import Box from "@mui/material/Box";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import { Link } from "react-router-dom";
import { useNavigationLinks } from "./hooks";

const item = {
    py: "2px",
    px: 3,
    color: "rgba(255, 255, 255, 0.7)",
    "&:hover, &:focus": {
        bgcolor: "rgba(255, 255, 255, 0.08)",
    },
};

const itemCategory = {
    boxShadow: "0 -1px 0 rgb(255,255,255,0.1) inset",
    py: 1.5,
    px: 3,
};

export default function Navigator(props: DrawerProps) {
    const { ...other } = props;

    const links = useNavigationLinks();

    return (
        <Drawer variant="permanent" {...other}>
            <List disablePadding>
                <ListItem sx={{ ...item, ...itemCategory, fontSize: 22, color: "#fff" }}>DMS</ListItem>
                <Box sx={{ bgcolor: "#101F33" }}>
                    <ListItem sx={{ py: 2, px: 3 }}>
                        <ListItemText sx={{ color: "#fff" }}>All</ListItemText>
                    </ListItem>
                    {links.map((link) => (
                        <Link to={link.path} key={link.id}>
                            <ListItem disablePadding key={link.id}>
                                <ListItemButton selected={link.active} sx={item}>
                                    <ListItemIcon>{link.icon}</ListItemIcon>
                                    <ListItemText>{link.id}</ListItemText>
                                </ListItemButton>
                            </ListItem>
                        </Link>
                    ))}
                    <Divider sx={{ mt: 2 }} />
                </Box>
            </List>
        </Drawer>
    );
}
